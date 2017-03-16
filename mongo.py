import io
import os
import time
import pickle
from pymongo import MongoClient

# Imports the Google Cloud client library
from google.cloud import speech
from google.cloud import storage
import google.cloud





def process_speech(file_name):
    mongo_client = MongoClient()
    text_db = mongo_client.s2t

    videos = text_db.videos
    speech_client = speech.Client()
    storage_client = storage.Client(project='speech-to-nlp')
    try:
        bucket = storage_client.get_bucket('galen_interviews')
    except google.cloud.exceptions.NotFound:
        print('Sorry, that bucket does not exist!')

    audio_sample = speech_client.sample(
        source_uri=file_name,
        encoding='LINEAR16',
        sample_rate=16000)
            #fit the model to the data

    print('Beginning speech processing...')
    operation = speech_client.speech_api.async_recognize(audio_sample)

    retry_count = 3000
    while retry_count > 0 and not operation.complete:
        retry_count -= 1
        time.sleep(2)
        operation.poll()
        if (retry_count%100 == 0):
            print('Processing...')

    if operation.complete:
        print('Operation complete!')
    else:
        print('Failed.')

    alternatives = operation.results

    for alternative in alternatives:
        text = ''
        text_dict = {}
        text+= alternative.transcript
        text_dict['text'] = text
        videos.insert_one(text_dict)


if __name__ == '__main__':
    # with io.open('gs_paths.txt', 'rb') as paths:
    #     for file_name in paths:
    for i in range(1,54):
        try:
            process_speech('gs://galen_interviews/file{}'.format(i)+'.raw')
            print('File {} completed!'.format(i))
            i +=1
        except:
            print("File {} failed to process.".format(i))
            i+=1
