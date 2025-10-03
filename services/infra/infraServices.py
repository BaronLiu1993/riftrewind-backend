import json
import io
import boto3

s3 = boto3.client('s3')


def uploadToS3Match(jsonData, bucket, objectName):
    try:
        json_string = json.dumps(jsonData)
        file_like_object = io.StringIO(json_string)        
        response = s3.put_object(Body=file_like_object.getvalue(), Bucket=bucket, Key=objectName)        
        print(response)
    except Exception as e:
        print(e)


def insertAllData(jsonData):
    try:
        uploadToS3Match(jsonData, "riftrewind", f"/match/{jsonData['matchId']}.json")
        print("completed")
    except Exception as e:
        print(e)


#uploadToS3Match(json_data, "riftrewind", "/match/test.json")