import boto3
import uuid
from dotenv import load_dotenv
import os
import awswrangler as wr
from io import StringIO
import random
import pandas as pd
import numpy as np



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

 
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, header=False, index=False)
    body = csv_buffer.getvalue()

    resp = sagemaker.invoke_endpoint(
        EndpointName="canvas-new-deployment-10-16-2025-6-05-PM",
        ContentType="text/csv",
        Body=body,
        EnableExplanations='`true`'
    )
    
    print(resp)


def callKnowledgeBase():
    pass

#Given These Stats give a funny description of what the player is 
#Input K means tells them what you are in terms of playstyle, converts it into funny meme 

#takes data too, given these data if the user won then what did they do good, if they lost what did they do poorly
def generateAgentInsights(prompt):
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

#given this data, analyse the trend and see how the player is doing after each match
def explainGraphs(prompt):
    pass


#executeAthenaQueryXGBoost("jzdg2rwr6k16dsjfalqjeixnhaa_yyffhr0xdpwqbzqieai2rpb4npjpd2zw_iibav31xmrtrz4p6g")
#print(generateAgentInsights("K means told me i am an aggressive laner give me a funny way to describe my playstyle. Be creative and include league references"))

