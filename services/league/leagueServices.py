import requests
from dotenv import load_dotenv
import os
import io
import boto3
import json

load_dotenv()

RIOT_API_KEY = os.environ.get("RIOT_API_KEY")
glue = boto3.client('glue', region_name='us-west-2')
s3 = boto3.client('s3')

def uploadToS3Match(jsonData, bucket, objectName):
    try:
        json_string = json.dumps(jsonData)
        file_like_object = io.StringIO(json_string)        
        response = s3.put_object(Body=file_like_object.getvalue(), Bucket=bucket, Key=objectName)        
        print(response)
    except Exception as e:
        print(e)


def insertDataMatch(jsonData, matchId, puuid):
    try:
        uploadToS3Match(jsonData, "riftrewind", f"match/{puuid}/{matchId}.json")
        print("completed")
    except Exception as e:
        print(e)

def retrieveAccountData(riotId: str, tag: str):
    try:
        url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{riotId}/{tag}?api_key={RIOT_API_KEY}"
        response = requests.get(url)
        data = response.json()
        print(data["puuid"])
        return data['puuid']
    except Exception as e:
        print(e)

def retrieveRankedData(PUUID: str):
    try:
        response = requests.get(f"https://na1.api.riotgames.com/lol/league/v4/entries/by-puuid/{PUUID}?api_key={RIOT_API_KEY}")
        data = response.json()
        print(data)
        uploadToS3Match(data, "riftrewind", f"player/{PUUID}/{PUUID}.json")    
        return data
    except Exception as e:
        print(e)

def retrieveEntriesData(PUUID: str):
    try:
        response = requests.get(f"https://na1.api.riotgames.com/lol/league/v4/entries/by-puuid/{PUUID}?api_key={RIOT_API_KEY}")
        data = response.json()
        return data
    except Exception as e:
        print(e)

def retrieveMatchIds(PUUID: str):
    try:
        response = requests.get(f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{PUUID}/ids?api_key={RIOT_API_KEY}")
        data = response.json()
        return data
    except Exception as e:
        print(e)

# Time Stamp DO the Same here and upload the data in the same format
def retrieveMatchData(matchId: str, puuid):
    try:
        response = requests.get(f"https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}?api_key={RIOT_API_KEY}")
        data = response.json()
        for i in range(len(data["info"]["participants"])):
            if data["info"]["participants"][i]["puuid"] == str(puuid):
                uploadToS3Match(data["info"]["participants"][i], "riftrewind", f"playerinput/{puuid}/{matchId}.json")
        #for i in range(len(data["info"]["teams"])):
        #    uploadToS3Match(data["info"]["teams"][i], "riftrewind", f"match/teams/{puuid}/{matchId}.json")

    except Exception as e:
        print(e)

#extract the correct PUUID too out from the output of the match data for each and aggregate as well
def retrieveMatchDataFramesTimeline(matchId: str, puuid: str):
    try:
        url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}/timeline?api_key={RIOT_API_KEY}"        
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json() 
        out_dir = os.path.join("timestamp", puuid)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{matchId}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data["info"]["frames"][len(data["info"]["frames"])], f, ensure_ascii=False, indent=2)
        #uploadToS3Match(data["info"], "riftrewind", f"timestamp/{puuid}/{matchId}.json")    

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

#Function inserts all necessary Data
def uploadAllDataToS3(riotId: str, tag: str):
    puuid = retrieveAccountData(riotId, tag)
    matchIdData = retrieveMatchIds(puuid)
    try:
        retrieveRankedData(puuid)
        for i in range(len(matchIdData)):
            retrieveMatchData(matchIdData[i], puuid)
        glue = boto3.client('glue', region_name='us-west-2')
        
        crawler_name = "riftrewindinputcrawler"
        glue.update_crawler(
            Name=crawler_name,
            Targets={
                "S3Targets": [
                    {"Path": f"s3://riftrewind/playerinput/{puuid}/"},
                ]
            }
        )        
        glue.start_crawler(Name=crawler_name)
       
    except:
        raise Exception("Failed")
       
    




