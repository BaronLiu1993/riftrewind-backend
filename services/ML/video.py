import random
import boto3
import json
import base64
import io

bedrock= boto3.client("bedrock-runtime", region_name="us-west-2")
s3 = boto3.client("s3", region_name = "us-west-2")
seed = random.randint(0, 2147483647)
BUCKET = "bucket_name"
IMAGE_KEY = "random_generated_name.png"

def generateStatsImage(prompt, puuid):
    native_request = {
    "taskType": "TEXT_IMAGE",
    "textToImageParams": {"text": prompt},
    "imageGenerationConfig": {
        "numberOfImages": 1,
        "quality": "standard",
        "cfgScale": 8.0,
        "height": 512,
        "width": 512,
        "seed": seed,
        },
    }

    request = json.dumps(native_request)
    response = bedrock.invoke_model(modelId="amazon-titan-image-generator-v1", body=request)
    image_data = base64.b64decode(response)

    MESSAGE = {"text": {"image": image_data}}
    data = json.loads(MESSAGE["text"])
    dec = base64.b64decode(data["image"])
    image_filelike = io.BytesIO(dec)
    s3.upload_fileobj(
        Fileobj=image_filelike,
        Bucket="/",
        Key=f"{puuid}.png",
        ExtraArgs={"ContentType":"image/png", "ACL":"public-read"},
        Callback=None,
        Config=None)

def generate_presigned_url(bucket_name, object_name, expiration=3600):
    try:
        response = s3.generate_presigned_url('put_object',
        Params={'Bucket': bucket_name,
        'Key': object_name},
        ExpiresIn=expiration)
    except Exception as e:
        print("Credentials not available")
        return None
    return response


    
    

