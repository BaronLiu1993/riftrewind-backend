from fastapi import FastAPI
from services.league.leagueServices import uploadAllDataToS3
from pydantic import BaseModel

app = FastAPI()

class SummonerRequest(BaseModel):
    riotId: str
    tag : str

@app.post()
async def ingestData(req: SummonerRequest):
    uploadAllDataToS3(req.riotId, req.tag)

@app.post()
async def queryChallengeStats():
    pass