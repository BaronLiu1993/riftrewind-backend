from fastapi import FastAPI
from services.league.leagueServices import uploadAllDataToS3
from services.athena.query import getMacroData
from pydantic import BaseModel

app = FastAPI()

class SummonerRequest(BaseModel):
    riotId: str
    tag : str

class PuuidRequest(BaseModel):
    puuid: str

@app.post()
async def ingestData(req: SummonerRequest):
    uploadAllDataToS3(req.riotId, req.tag)

@app.post()
async def getMacroData(req: PuuidRequest):
    getMacroData(req.puuid)

@app.post()
async def queryChallengeStats():
    pass