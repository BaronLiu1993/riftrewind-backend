from fastapi import FastAPI
from services.league.leagueServices import uploadAllDataToS3
from services.athena.query import getMacroData
from services.ML.ML import callAgent
from pydantic import BaseModel
import json

app = FastAPI()

class SummonerRequest(BaseModel):
    riotId: str
    tag : str

class PuuidRequest(BaseModel):
    puuid: str

class MacroData(BaseModel):
    data: str

@app.post("/ingest")
async def ingestData(req: SummonerRequest):
    try:
        uploadAllDataToS3(req.riotId, req.tag)
    except Exception as e:
        print(e)

@app.post("/macrodata")
async def retrieveMacroData(req: PuuidRequest):
    try:
        data = getMacroData(req.puuid)
        outputJson = json.loads(data)
        return outputJson   
    except Exception as e:
        print(e)

@app.post("/analyse/macrodata")
async def analyseMacroData(req: MacroData):
    try:
        data = callAgent(req.data)
        return data
    except Exception as e:
        print(e)


@app.post("/macrodata/generate/graphs")
async def generateMacroDrafts(req: MacroData):
    try:
        graphData = callAgent(req.data)
        return graphData
    except Exception as e:
        print(e)
