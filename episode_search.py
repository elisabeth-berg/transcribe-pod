import boto3
import json
import os
import pandas as pd

ACCESS_KEY = json.load(open("access_key.json"))['access_key']
SECRET_KEY = json.load(open("access_key.json"))['secret_key']

s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )

def search_episodes(search_phrase, bucket_name="pod-transcription-storage", context_window=20):
    transcriptions = [
        f.key for f in s3.Bucket(bucket_name).objects.all()
        if f.key[-4:]=="json"
    ]
    search_phrase = search_phrase.lower()
    first_word = search_phrase.split(' ')[0]
    match_df = pd.DataFrame(
        columns=["filename", "content", "confidence", "start_time", "context"]
    )
    for episode in transcriptions:
        transcript_df = json_parse(episode)
        match_rows = transcript_df[transcript_df.content == first_word]
        if len(match_rows) > 0:
            context = [
                " ".join(transcript_df[
                    (transcript_df.index > i-context_window)
                    & (transcript_df.index < i+context_window)
                ]['content'].values) for i in match_rows.index]
            match_rows["context"] = context
            # Only return rows containing the entire search phrase
            match_rows = match_rows[match_rows["context"].str.contains(search_phrase)]
            match_df = pd.concat([match_df, match_rows])
            print(
                "{}\nFirst appearance at {}:\n\"{}\"\n---------------------".format(
                    episode,
                    match_rows.iloc[0]["start_time"],
                    match_rows.iloc[0]["context"]
                )
            )
    return match_df

def json_parse(episode, transcript_dir="pod-transcription-storage"):
    transcript = json.loads(
        s3.Object(transcript_dir, episode).get()["Body"].read().decode("utf-8")
    )
    transcript_words = transcript["results"]["items"]
    transcript_df = pd.DataFrame(transcript_words)
    transcript_df = transcript_df[transcript_df["type"] == "pronunciation"]
    transcript_df["filename"] = episode[:-5]
    transcript_df["confidence"] = transcript_df['alternatives'].apply(lambda x: x[0]["confidence"])
    transcript_df["content"] = transcript_df['alternatives'].apply(lambda x: x[0]["content"].lower())
    transcript_df = transcript_df.drop(columns = ["alternatives", "type", "end_time"])
    return transcript_df
