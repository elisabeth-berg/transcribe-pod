import boto3
import json
import os
import pandas as pd

ACCESS_KEY = json.load(open("access_key.json"))['access_key']
SECRET_KEY = json.load(open("access_key.json"))['secret_key']

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    )

def search_episodes(search_phrase, transcript_files, context_window=20):
    search_phrase = search_phrase.lower()
    first_word = search_phrase.split(' ')[0]
    match_df = pd.DataFrame(columns=["filename", "content", "confidence", "start_time", "context"])
    for episode_name in episode_names:
        transcript_df = json_parse(episode_name)
        match_rows = transcript_df[transcript_df.content == first_word]
        context = [
            " ".join(transcript_df[
                (transcript_df.index > i-context_window)
                & (transcript_df.index < i+context_window)
            ]['content'].values) for i in match_rows.index]
        match_rows["context"] = context
        # Only return rows containing the entire search phrase
        match_rows = match_rows[match_rows["context"].str.contains(search_phrase)]
        match_df = pd.concat(match_df, match_rows)
    return match_df

def json_parse(episode_name, transcript_dir="pod-transcription-storage"):
    with open(os.path.join(transcript_dir, episode_name), 'r') as f:
        transcript = json.loads(f.read())

    transcript_words = transcript["results"]["items"]
    transcript_df = pd.DataFrame(transcript_words)
    transcript_df = transcript_df[transcript_df["type"] == "pronunciation"]
    transcript_df["filename"] = transcript_file
    transcript_df["confidence"] = transcript_df['alternatives'].apply(lambda x: x[0]["confidence"])
    transcript_df["content"] = transcript_df['alternatives'].apply(lambda x: x[0]["content"].lower())
    transcript_df = transcript_df.drop(columns = ["alternatives", "type", "end_time"])
    return transcript_df
