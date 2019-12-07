# Transcribe-Pod

A project that transcribes podcasts

## Setting up

### Installing

```
git clone https://github.com/elisabeth-berg/transcribe-pod.git
```

```
pip install -r requirements.txt
```

### Environment Variables
- access_key (aws access key id)
- secret_key (aws secret access key)

### Updating Environment

```
pip freeze > requirements.txt
```

## Functions

- full_scrape(episode_directory)
  - Download every episode of Cooking Issues!
  - Starts from the homepage given, and follows the pagination until there
  are no further pages remaining.
  - Params: **episode_directory** : string, full or relative directory where the episodes
    will be stored
