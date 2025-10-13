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
    uploadAllDataToS3(req.riotId, req.tag)

@app.post("/macrodata")
async def retrieveMacroData(req: PuuidRequest):
    data = getMacroData(req.puuid)
    outputJson = json.loads(data)
    return outputJson

@app.post("/analyse/macrodata")
async def analyseMacroData(req: MacroData):
    data = callAgent()
