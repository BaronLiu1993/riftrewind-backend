import boto3
import uuid
from dotenv import load_dotenv
import os
import awswrangler as wr
from io import StringIO
import pandas as pd
import json 
import re



s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-agent-runtime', region_name="us-west-2")
sagemaker = boto3.client("sagemaker-runtime", region_name="us-west-2")

load_dotenv()

agentId = os.environ.get("AGENT_ID")
agentAliasId = os.environ.get("AGENT_ALIAS_ID")


#Get the data from athena that is aggregated and then insert into athena
def executeAthenaQueryKMeans(puuid):
    #Make Athena Query
    query = f"""
WITH base AS (
  SELECT
    COALESCE(NULLIF(teamposition,''), NULLIF(role,'')) AS role_std,
    timeplayed, goldearned,
    totalminionskilled, totalallyjungleminionskilled, totalenemyjungleminionskilled,
    totaldamagedealttochampions, visionscore, wardsplaced, wardskilled, deaths
  FROM {puuid}
  WHERE timeplayed BETWEEN 900 AND 3600
),
feats AS (
  SELECT
    role_std,
    goldearned / (timeplayed / 60.0) AS gpm,
    (totalminionskilled + totalallyjungleminionskilled + totalenemyjungleminionskilled) / (timeplayed / 60.0) AS cs_pm,
    totaldamagedealttochampions / (timeplayed / 60.0) AS dmg_pm,
    visionscore / (timeplayed / 60.0) AS vision_pm,
    wardsplaced / (timeplayed / 60.0) AS wards_placed_pm,
    wardskilled / (timeplayed / 60.0) AS wards_killed_pm,
    deaths / (timeplayed / 60.0) AS deaths_pm
  FROM base
)
SELECT
  (gpm - avg(gpm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(gpm) OVER (PARTITION BY role_std), 0) AS z_gpm,
  (cs_pm - avg(cs_pm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(cs_pm) OVER (PARTITION BY role_std), 0) AS z_cs_pm,
  (dmg_pm - avg(dmg_pm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(dmg_pm) OVER (PARTITION BY role_std), 0) AS z_dmg_pm,
  (vision_pm - avg(vision_pm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(vision_pm) OVER (PARTITION BY role_std), 0) AS z_vision_pm,
  (wards_placed_pm - avg(wards_placed_pm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(wards_placed_pm) OVER (PARTITION BY role_std), 0) AS z_wards_placed_pm,
  (wards_killed_pm - avg(wards_killed_pm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(wards_killed_pm) OVER (PARTITION BY role_std), 0) AS z_wards_killed_pm,
  (deaths_pm - avg(deaths_pm) OVER (PARTITION BY role_std)) / NULLIF(stddev_samp(deaths_pm) OVER (PARTITION BY role_std), 0) AS z_deaths_pm
FROM feats
WHERE gpm IS NOT NULL AND cs_pm IS NOT NULL AND dmg_pm IS NOT NULL
  AND vision_pm IS NOT NULL AND wards_placed_pm IS NOT NULL
  AND wards_killed_pm IS NOT NULL AND deaths_pm IS NOT NULL;

"""
    df = wr.athena.read_sql_query(
        sql=query,
        database="riftrewindinput",
        boto3_session=boto3.Session(region_name='us-west-2')
    )

    print(df.head())
    X = df[['z_gpm','z_cs_pm','z_dmg_pm','z_vision_pm','z_wards_placed_pm','z_wards_killed_pm','z_deaths_pm']].to_numpy(dtype=float)

    """
    resp = sagemaker.invoke_endpoint(
        EndpointName="kmeansjungendpoint",           
        ContentType="text/csv",                     
        Accept="application/json",                   
        Body=payload.encode("utf-8")
    )
    """

def executeAthenaQueryXGBoost(puuid):
    query = f"""
SELECT
  allinpings, assistmepings, assists, baronkills, basicpings, bountylevel,
  champexperience, champlevel, championtransform, commandpings, 
  consumablespurchased, damagedealttobuildings, damagedealttoobjectives, 
  damagedealttoturrets, damageselfmitigated, dangerpings, deaths, 
  detectorwardsplaced, doublekills, dragonkills,
  CAST(firstbloodassist AS INTEGER) AS firstbloodassist,
  CAST(firstbloodkill AS INTEGER) AS firstbloodkill,
  CAST(firsttowerassist AS INTEGER) AS firsttowerassist,
  CAST(firsttowerkill AS INTEGER) AS firsttowerkill,
  goldearned, goldspent, itemspurchased,
  item0, item1, item2, item3, item4, item5, item6,
  killingsprees, kills, largestcriticalstrike, largestkillingspree, 
  largestmultikill, longesttimespentliving,
  magicdamagedealt, magicdamagedealttochampions, magicdamagetaken,
  physicaldamagedealt, physicaldamagedealttochampions, physicaldamagetaken,
  truedamagedealt, truedamagedealttochampions, truedamagetaken,
  needvisionpings, neutralminionskilled, objectivesstolen, 
  objectivesstolenassists, sightwardsboughtingame,
  totaldamagedealt, totaldamagedealttochampions, totaldamageshieldedonteammates,
  totaldamagetaken, totalenemyjungleminionskilled, totalheal, 
  totalhealsonteammates, totalminionskilled, totaltimeccdealt,
  totaltimespentdead, totalunitshealed, triplekills,
  turretkills, turrettakedowns, inhibitorkills, inhibitortakedowns,
  visionclearedpings, visionscore, visionwardsboughtingame,
  wardskilled, wardsplaced,
  spell1casts, spell2casts, spell3casts, spell4casts,
  summoner1casts, summoner2casts,
  enemymissingpings, enemyvisionpings, getbackpings, holdpings,
  onmywaypings, pushpings, retreatpings,
  LOWER(championname) AS championname,
  LOWER(individualposition) AS individualposition,
  LOWER(lane) AS lane,
  LOWER(role) AS role,
  LOWER(teamposition) AS teamposition,
  timeccingothers, timeplayed,
  placement, playersubteamid, subteamplacement
FROM {puuid}
WHERE championname IS NOT NULL
  AND timeplayed > 0
  AND champlevel > 0;
"""
    df = wr.athena.read_sql_query(
        sql=query,
        database="riftrewindinput",
        boto3_session=boto3.Session(region_name='us-west-2')
    )
    return df

def executeXGBoost(df):
    original = df.copy()
    
    encoded = df.copy()
    for col in ['championname', 'individualposition', 'lane', 'role', 'teamposition']:
        if col in encoded.columns:
            dummies = pd.get_dummies(encoded[col], prefix=col, drop_first=True)
            encoded = pd.concat([encoded.drop(col, axis=1), dummies], axis=1)
    encoded = encoded.fillna(0)
    
    csv_buffer = StringIO()
    encoded.to_csv(csv_buffer, header=False, index=False)
    
    response = sagemaker.invoke_endpoint(
        EndpointName="canvas-new-deployment-10-16-2025-6-05-PM",
        ContentType='text/csv',
        Body=csv_buffer.getvalue()
    )
    
    result = response['Body'].read().decode('utf-8')
    predictions = []    
    for idx, line in enumerate(result.strip().split('\n')):
        parts = re.match(r'(\d+),([\d.]+),"(\[.*?\])","(\[.*?\])"', line)
        
        if parts:
            pred_class = int(parts.group(1))
            probs = json.loads(parts.group(3))
            labels = json.loads(parts.group(4).replace("'", '"'))
            
            win_prob = float(dict(zip(labels, probs)).get('1', probs[1]))
            
            predictions.append({
                'match_id': idx,
                'champion': str(original.iloc[idx].get('championname', 'unknown')),
                'win_probability': round(win_prob, 4),
                'predicted_outcome': 'WIN' if pred_class == 1 else 'LOSS',
                'top_features': [
                    {'name': col, 'value': original.iloc[idx][col]}
                    for col in ['kills', 'deaths', 'assists', 'goldearned', 
                               'totaldamagedealt', 'totalminionskilled', 
                               'visionscore', 'turretkills', 'baronkills', 'dragonkills']
                    if col in original.columns and original.iloc[idx][col] > 0
                ][:10]
            })
    
    return json.dumps(predictions, indent=2)

def callKnowledgeBase():
    pass

#Given These Stats give a funny description of what the player is 
#Input K means tells them what you are in terms of playstyle, converts it into funny meme 
#takes data too
def callAgent(prompt):
    session_id = str(uuid.uuid4())
    try:
        resp = bedrock.invoke_agent(
            agentId=agentId,
            agentAliasId=agentAliasId,
            sessionId=session_id,
            inputText=prompt,
            streamingConfigurations={"streamFinalResponse": False},
        )

        chunks = []
        for event in resp["completion"]:
            if "chunk" in event:
                chunks.append(event["chunk"]["bytes"].decode("utf-8", errors="ignore"))
        return "".join(chunks)
    except Exception as e:
        raise Exception(e)
    
"""
def videoGenerationJob(bedrock_runtime, prompt, output_s3_uri):
    model_id = "amazon.nova-reel-v1:0"
    seed = random.randint(0, 2147483646)
    model_input = {
        "taskType": "TEXT_VIDEO",
        "textToVideoParams": {"text": prompt},
        "videoGenerationConfig": {
            "fps": 24,
            "durationSeconds": 6,
            "dimension": "1280x720",
            "seed": seed,
        },
    }

    # Specify the S3 location for the output video
    output_config = {"s3OutputDataConfig": {"s3Uri": output_s3_uri}}

    # Invoke the model asynchronously
    response = bedrock_runtime.start_async_invoke(
        modelId=model_id, modelInput=model_input, outputDataConfig=output_config
    )

    invocation_arn = response["invocationArn"]

    return invocation_arn


def query_job_status(bedrock_runtime, invocation_arn):
    return bedrock_runtime.get_async_invoke(invocationArn=invocation_arn)


def main():

    # Create a Bedrock Runtime client
    # Note: Credentials will be loaded from the environment or AWS CLI config
    bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

    # Configure the text prompt and output location
    prompt = "Closeup of a cute old steampunk robot. Camera zoom in."

    # Verify the S3 URI has been set to a valid bucket
    if "REPLACE-WITH-YOUR-S3-BUCKET-NAME" in OUTPUT_S3_URI:
        print("ERROR: You must replace the OUTPUT_S3_URI with your own S3 bucket URI")
        return

    print("Submitting video generation job...")
    invocation_arn = start_text_to_video_generation_job(
        bedrock_runtime, prompt, OUTPUT_S3_URI
    )
    print(f"Job started with invocation ARN: {invocation_arn}")

    # Poll for job completion
    while True:
        print("\nPolling job status...")
        job = query_job_status(bedrock_runtime, invocation_arn)
        status = job["status"]

        if status == "Completed":
            bucket_uri = job["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"]
            print(f"\nSuccess! The video is available at: {bucket_uri}/output.mp4")
            break
        elif status == "Failed":
            print(
                f"\nVideo generation failed: {job.get('failureMessage', 'Unknown error')}"
            )
            break
        else:
            print("In progress. Waiting 15 seconds...")
            time.sleep(15)
"""

test = executeAthenaQueryXGBoost("jzdg2rwr6k16dsjfalqjeixnhaa_yyffhr0xdpwqbzqieai2rpb4npjpd2zw_iibav31xmrtrz4p6g")
print(executeXGBoost(test))
#print(callAgent("K means told me i am an aggressive laner give me a funny way to describe my playstyle. Be creative and include league references"))
#executeAthenaQueryKMeans("jzdg2rwr6k16dsjfalqjeixnhaa_yyffhr0xdpwqbzqieai2rpb4npjpd2zw_iibav31xmrtrz4p6g")
