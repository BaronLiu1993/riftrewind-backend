from fastapi import FastAPI

app = FastAPI()

@app.get()
async def insertIntoS3():
    pass

@app.post()
async def dataIngest():
    return