import boto3
import json
import time


ACCESS_KEY = json.load(open("access_key.json"))['access_key']
SECRET_KEY = json.load(open("access_key.json"))['secret_key']

transcribe = boto3.client(
    'transcribe',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )


def transcribe_all():
    job_name = "test_episodes2"
    bucket_name = "pod-transcription-storage"
    prefix = "episodes"
    region = "us-west-2"
    job_uris = get_uris(bucket_name, prefix, region)

    for job_uri in job_uris:
        transcribe_one(bucket_name, job_name, job_uri)


def get_uris(bucket_name, prefix, region):
    episodes = [f["Key"] for f in s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)["Contents"]]
    uris = ["https://{}.s3-{}.amazonaws.com/{}".format(bucket_name, region, e) for e in episodes]
    return uris

def transcribe_one(bucket_name, job_name, job_uri):
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat='mp3',
        LanguageCode='en-US',
        OutputBucketName="{}/transcriptions".format(bucket_name),
    )
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(5)
    print(status)
