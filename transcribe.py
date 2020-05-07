import boto3
import json
import os
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
s3_resource = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)


def transcribe_all():
    bucket_name = "pod-transcription-storage"
    prefix = "episodes"
    region = "us-west-2"
    episode_names, job_uris = get_uris(bucket_name, prefix, region)
    current_transcriptions = [
        f["Key"] for f in s3.list_objects_v2(Bucket=bucket_name)["Contents"]
        if f["Key"][-4:]=="json"
    ]
    for (episode_name, job_uri) in zip(episode_names, job_uris):
        if episode_name in current_transcriptions:
            print("{} has already been transcribed".format(episode_name))
        else:
            transcribe_one(bucket_name, episode_name, job_uri)


def get_uris(bucket_name, prefix, region):
    episode_names = [f["Key"].split('/')[1][:-4] for f in s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)["Contents"]]
    uris = ["https://{}.s3-{}.amazonaws.com/{}/{}.mp3".format(bucket_name, region, prefix, e) for e in episode_names]
    return episode_names, uris

def transcribe_one(bucket_name, episode_name, job_uri):
    print("Starting {}".format(job_uri))
    transcribe.start_transcription_job(
        TranscriptionJobName=episode_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat='mp3',
        LanguageCode='en-US',
        OutputBucketName=bucket_name,
    )
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=episode_name)
        print(status)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(5)

if __name__ == "__main__":
#    transcribe_all()
    bucket_name = "pod-transcription-storage"
    prefix = "episodes"
    region = "us-west-2"
    episode_names, job_uris = get_uris(bucket_name, prefix, region)
    for episode_name, job_uri in zip(episode_names[:2], job_uris[:2]):
        transcribe_one(bucket_name, episode_name, job_uri)
