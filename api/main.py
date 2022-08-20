from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
import urllib.request
from google.cloud import storage
import os
import socket
import requests

app = FastAPI()

class Params(BaseModel):
    url: str
    bucket_name: str
    output_file_prefix: str

def put_file_to_gcs(output_file:str, bucket_name:str, content):
    try:

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_file)
        blob.upload_from_string(content)

        return 'OK'
    except Exception as ex:
        print(ex)

@app.get("/")
async def read_root():
    return {"Hello": "World"}

def get_dados(remote_url):
    response = requests.get(remote_url)

    return response

@app.post("/download_combustivel")
async def download_combustivel(params: Params):
    try:
        
        print(os.listdir())

        data = get_dados(params.url)

        put_file_to_gcs(bucket_name=params.bucket_name, 
                            output_file=params.output_file_prefix, 
                            content=data.content)


        return {"Status": "OK", "Bucket_name": params.bucket_name, "url": params.url}
    except Exception as ex:
        raise HTTPException(status_code=ex.code, detail=f"{ex}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)