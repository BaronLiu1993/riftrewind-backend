from fastapi import FastAPI
from services.league.leagueServices import uploadAllDataToS3
from services.athena.query import getMacroData, generateQualitativeStatsGraphData, generateQuantitativeStatsGraphData
from services.ML.ML import generateAgentInsights
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
        data = generateAgentInsights(req.data)
        return data
    except Exception as e:
        print(e)

@app.post("/graphs/quantitative")
async def generateMacroDrafts(req: PuuidRequest):
    try:
        data = generateQuantitativeStatsGraphData(req.puuid)
        outputJson = json.loads(data)
        return outputJson
    except Exception as e:
        print(e)

@app.post("/graphs/qualitative")
async def generateMacroDrafts(req: PuuidRequest):
    try:
        data = generateQualitativeStatsGraphData(req.puuid)
        outputJson = json.loads(data)
        return outputJson
    except Exception as e:
        print(e)
