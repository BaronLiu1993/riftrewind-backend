import boto3
import uuid

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-agent-runtime', region_name="us-west-2")

def callAgent(prompt):
    session_id = str(uuid.uuid4())
    try:
        resp = bedrock.invoke_agent(
            agentId="7QIO2RK8QC",
            agentAliasId="UEHVTCLAZV",
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
