import boto3
import uuid
from dotenv import load_dotenv
import os
import awswrangler as wr
import random
import time


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

def executeAthenaQueryXGBoost(bucketPath):
    pass

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
    """
    Queries the status of an asynchronous video generation job.

    :param bedrock_runtime: The Bedrock runtime client
    :param invocation_arn: The ARN of the async invocation to check

    :return: The runtime response containing the job status and details
    """
    return bedrock_runtime.get_async_invoke(invocationArn=invocation_arn)


def main():
    """
    Main function that demonstrates the complete workflow for generating
    a video from a text prompt using Amazon Nova Reel.
    """
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

#print(callAgent("K means told me i am an aggressive laner give me a funny way to describe my playstyle. Be creative and include league references"))
#executeAthenaQueryKMeans("jzdg2rwr6k16dsjfalqjeixnhaa_yyffhr0xdpwqbzqieai2rpb4npjpd2zw_iibav31xmrtrz4p6g")
