import boto3
import uuid
from dotenv import load_dotenv
import os
from sagemaker import image_uris

kmeans_image = image_uris.retrieve(framework="kmeans", region="us-east-1") 

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-agent-runtime', region_name="us-west-2")
sagemaker = boto3.client('sagemaker')
load_dotenv()

agentId = os.environ.get("AGENT_ID")
agentAliasId = os.environ.get("AGENT_ALIAS_ID")


#Get the data from athena that is aggregated and then insert into athena
def executeAthenaQueryKMeans(bucketPath):
    pass

def executeAthenaQueryXGBoost(bucketPath):
    pass

def callKnowledgeBase():
    pass

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


print(callAgent("Tell me in one short sentence what is the best way to get better at league of legends?"))
#print(trainkMeansCluster())