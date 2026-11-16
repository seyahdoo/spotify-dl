import json
import os

import mongita.collection
from dotenv import load_dotenv, dotenv_values
load_dotenv("secrets.env")

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import requests
import json
import time
from pathlib import Path
import string
import pymongo
# from windows_metadata import WindowsAttributes
import pickle

from spotify_scraper import SpotifyClient
# https://github.com/AliAkhtari78/SpotifyScraper

DOWNLOADS_FOLDER = "/mnt/spotify-dl"
# DOWNLOADS_FOLDER = "downloads"

pymongo_client = pymongo.MongoClient("mongodb://root:example@192.168.1.42:27017/", 27017)
pymongo_db = pymongo_client.db
songs_collection = pymongo_db.songs_collection
keys_collection = pymongo_db.keys_collection

def main():
    # add_all_not_found_songs_to_a_playlist()
    # get_songs_from_found_folder()
    # find_tracks_on_folders(songs_collection)
    # update_playlists()
    
    return 
    while True:
        queue_update(songs_collection, keys_collection)
        poll_downloads(songs_collection, keys_collection)
        update_playlists()
        time.sleep(5)
    return    

def get_songs_from_found_folder():
    found_folder = os.path.join(DOWNLOADS_FOLDER, "found")
    # attributes = {}
    # for file in os.listdir(found_folder):
    #     file_path = os.path.join(found_folder, file)
    #     attributes[file_path] = WindowsAttributes(Path(file_path))
    #     print(file_path)
    # 
    # attributes_slim = {}
    # for ke in attributes.keys():
    #     attributes_slim[ke] = {}
    #     attributes_slim[ke]["Title"] = attributes[ke]["Title"]
    #     attributes_slim[ke]["Album artist"] = attributes[ke]["Album artist"]
    #     print(ke)
    #     
    # pickle.dump(attributes_slim, open('attributes.pkl', 'wb'))
    
    attributes = pickle.load(open('attributes.pkl', 'rb'))

    for file in os.listdir(found_folder):
        file_path = os.path.join(found_folder, file)
        attr = attributes[file_path]
        file_title = attr["Title"]
        filter = {"status": "not_found", "song_name": file_title}
        count = songs_collection.count_documents(filter)
        if count == 1:
            print("found pair")
            song = songs_collection.find_one(filter)
            supposed_path = get_file_path(song)
            directory = os.path.dirname(supposed_path)
            Path(directory).mkdir(parents=True, exist_ok=True)
            os.rename(file_path, supposed_path)
        else:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # songs = songs_collection.find(filter)
        
    
    
    # songs = songs_collection.find({"status": "not_found"})
    # for song in songs:
    #     song_name = song["song_name"]
    #     artist_name = song["artist_name"]
    #     found = False
    #     for file in os.listdir(found_folder):
    #         file_path = os.path.join(found_folder, file)
    #         attr = attributes[file_path]
    #         file_title = attr["Title"]
    #         file_artist = attr["Album artist"]
    #         if song_name == file_title and artist_name == file_artist:
    #             supposed_path = get_file_path(song)
    #             directory = os.path.dirname(supposed_path)
    #             Path(directory).mkdir(parents=True, exist_ok=True)
    #             os.rename(file_path, supposed_path)
    #             print(f"{song_name} - {artist_name}")
    #             found = True
    #             break
    #     if not found:
    #         print(f"{song_name} - {artist_name}")
    #     else:
    #         print("found")
    

    return 

def add_all_not_found_songs_to_a_playlist():
    scope = "user-library-read, playlist-read-private, playlist-modify-public"
    spotipy_client = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    # playlist = spotipy_client.current_user_playlist_create("missing tracks")
    #'2PLwauuoo3Q9DUc7lwbyqi'
    songs = songs_collection.find({"status": "not_found"})
    for song in songs:
        spotipy_client.playlist_add_items('2PLwauuoo3Q9DUc7lwbyqi', [song["track_id"]])
        pass
    return 

def retry_all():
    songs = songs_collection.find({"status": "errored"})
    for song in songs:
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "ready_to_start"}})
    songs = songs_collection.find({"status": "not_found"})
    for song in songs:
       songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "ready_to_start"}})
    return 

def process_songs_to_be_ingested():
    scope = "user-library-read, playlist-read-private, playlist-modify-public"
    spotipy_client = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
    with SpotifyClient() as open_spotify_client:
        process_txt("playlists-sidal.txt", "sidal", songs_collection, spotipy_client, open_spotify_client)
    return 

def release_key(song):
    key = keys_collection.find_one({"_id": song["key_id"]})
    keys_collection.update_one({"_id": key["_id"]}, {'$set': {"current_download_task_count": key["current_download_task_count"] - 1}})
    songs_collection.update_one({"_id": song["_id"]}, { "$unset" : { "key_id" : 1}})
    return 

def register_keys(db):
    keys_collection:pymongo.collection.Collection = db.keys_collection
    keys_collection.insert_one({"key_id": 1, "secret": os.environ.get("SPODOWN_API_KEY_1"), "current_download_task_count": 0})
    keys_collection.insert_one({"key_id": 2, "secret": os.environ.get("SPODOWN_API_KEY_2"), "current_download_task_count": 0})
    keys_collection.insert_one({"key_id": 3, "secret": os.environ.get("SPODOWN_API_KEY_3"), "current_download_task_count": 0})
    return 

def move_missing_tracks_to_ready_to_start(songs_collection):
    missing_songs = songs_collection.find({"status": "found_in_disk"})
    for song in missing_songs:
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "done"}})
    return    

def find_tracks_on_folders(songs_collection):
    # for each song in done, make double sure it exists in the disk
    # done_songs = songs_collection.find({"status": "ready_to_start"})
    done_songs = songs_collection.find({"status": "not_found"})
    for song in done_songs:
        folder = get_track_folder(song)
        track_number = song["track_number"]
        found = False
        try:
            for file in os.listdir(folder):
                if file.startswith(str(track_number)):
                    found = True
        except Exception as e:
            pass
        if found:
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "done"}})
    return 
    
def update_playlists():
    for root, dirs, files in os.walk(DOWNLOADS_FOLDER):
        playlist = ""
        for n in files:
            n: str
            if n.endswith(".mp3"):
                playlist += n + "\n"
        if playlist.count("\n") >= 1:
            folder_name = os.path.basename(os.path.normpath(root))
            playlist += folder_name + ".m3u" + "\n"
            playlist_file_path = os.path.join(root, folder_name + ".m3u")
            with open(playlist_file_path, "w") as f:
                f.write(playlist)
    
    return 

def process_txt(text_file_name, owner, songs_collection, spotipy_client, open_spotify_client):
    with open(text_file_name, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            # - [ ] https://open.spotify.com/playlist/51NWUHnNVDi0VCm4dtuuhr
            # - [ ] https://open.spotify.com/playlist/0aBexwpyH0KmaxUQdD9eUF?si=aGr6g7CWSgqcZLJPCtVXsA
            if "/playlist/" in line:
                playlist_id = line.split("/")[-1].split("?")[0].strip()
                print(playlist_id)
                try:
                    ingest_track_ids_open_spotify(owner, songs_collection, open_spotify_client, playlist_id)
                except:
                    print(f"failed to scrape with open spotify, trying with spotify api {playlist_id}")
                    ingest_track_ids_spotipy(owner, songs_collection, spotipy_client, playlist_id)
    return

def ingest_track_ids_spotipy(owner, songs_collection, spotipy_client, playlist_id):
    time.sleep(.5)
    playlist_data = spotipy_client.playlist(playlist_id)
    playlist_name = playlist_data["name"]
    print(playlist_name)
    time.sleep(.5)
    results = spotipy_client.playlist_items(playlist_id, market="TR")
    while results:
        for index, item in enumerate(results["items"]):
            track_id = item["item"]["id"]
            song_name = item["item"]["name"]
            artist_name = item["item"]["artists"][0]["name"]
            track_number = index + 1
            document = {
                "owner": owner,
                "track_id": track_id,
                "playlist_name": playlist_name,
                "song_name": song_name,
                "artist_name": artist_name,
                "track_number": track_number,
                "status": "ready_to_start",
                "download_url": "",
                "download_job_id": "",
                "source": "spotipy",
            }
            add_song_to_collection(songs_collection, document)
        if results["next"]:
            time.sleep(.5)
            results = spotipy_client.next(results)
        else:
            break
    return

def ingest_track_ids_open_spotify(owner, songs_collection, open_spotify_client, playlist_id):
    time.sleep(.5)
    playlist = open_spotify_client.get_playlist(f"https://open.spotify.com/playlist/{playlist_id}", max_tracks=10000)
    playlist_name = playlist.name
    print(playlist_name)
    time.sleep(.5)
    for index, track in enumerate(playlist.tracks):
        track_id = track.track.id
        song_name = track.track.name
        artist_name = track.track.artists[0].name
        track_number = index + 1
        document = {
            "owner": owner, 
            "track_id": track_id, 
            "playlist_name": playlist_name,
            "song_name": song_name,
            "artist_name": artist_name,
            "track_number": track_number,
            "status": "ready_to_start",
            "download_url": "",
            "download_job_id": "",
            "source": "open_spotify",
        }
        add_song_to_collection(songs_collection, document)
    return

def add_song_to_collection(song_collection:mongita.collection.Collection, song):
    found = song_collection.find_one({
        "owner": {"$eq":song["owner"]},
        "playlist_name": {"$eq":song["playlist_name"]},
        "track_id": {"$eq":song["track_id"]},
    })
    if found is not None:
        return
    
    song_collection.insert_one(song)
    print(f"added song: {song_to_string(song)}")
    return 

def song_to_string(song):
    return f"{song["owner"]} - {song["playlist_name"]} - {song["track_number"]} - {song["song_name"]} - {song["artist_name"]} - {song["track_id"]}"

def get_file_path(document):
    return f"{DOWNLOADS_FOLDER}/{document["owner"]}/{format_filename(document["playlist_name"])}/{document["track_number"]} - {format_filename(document["song_name"])} - {format_filename(document["artist_name"])}.mp3"

def get_track_folder(document):
    return f"{DOWNLOADS_FOLDER}/{document["owner"]}/{format_filename(document["playlist_name"])}"

def queue_update(songs_collection: mongita.collection.Collection, keys_collection: mongita.collection.Collection):
    keys = keys_collection.find()
    for key in keys:
        if key["current_download_task_count"] < 2:
            queue_a_song_with_key(songs_collection, keys_collection, key)
    return 

def queue_a_song_with_key(songs_collection: mongita.collection.Collection, keys_collection: mongita.collection.Collection, key):
    song_to_download = songs_collection.find_one({"status": {"$eq":"ready_to_start"}})
    if song_to_download is not None:
        queue_download_track(songs_collection, keys_collection, song_to_download, key)
        return
    return 

def queue_download_track(songs_collection: mongita.collection.Collection, keys_collection: mongita.collection.Collection, song, key):
    headers = {
        'X-Api-Key': key["secret"],
        'Content-Type': 'application/json',
    }
    json_data = {
        'spotify_url': f'https://open.spotify.com/track/{song["track_id"]}',
        'format': 'mp3-320',
    }
    try:
        time.sleep(1)
        response = requests.post('https://hunt.spodown.com/api/v1/ext/downloads', headers=headers, json=json_data)
        response_json = json.loads(response.text)
        if "code" in response_json and response_json["code"] == "track_unplayable":
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "not_found"}})
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"error_message": response.text}})
            print(f"not found {song_to_string(song)}")
            return
        if "code" in response_json and response_json["code"] == "track_not_found":
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "not_found"}})
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"error_message": response.text}})
            print(f"not found {song_to_string(song)}")
            return
        response.raise_for_status()
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"download_job_id": response_json["job_id"]}})
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "downloading"}})
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"key_id": key["_id"]}})
        keys_collection.update_one({"_id": key["_id"]}, {'$set': {"current_download_task_count": key["current_download_task_count"] + 1}})
        print(f"queued {song_to_string(song)}")
    except Exception:
        print(f"error queueing {song_to_string(song)}")
    return
    
def poll_downloads(songs_collection:mongita.collection.Collection, keys_collection):
    downloading_songs = songs_collection.find({"status": {"$eq": "downloading"}})
    for song in downloading_songs:
        url = check_download_ready(songs_collection, keys_collection, song)
        if url is not None:
            download_track(songs_collection, keys_collection, song, url)
    return 

def check_download_ready(songs_collection:mongita.collection.Collection, keys_collection:mongita.collection.Collection, song):
    headers = {
        'X-Api-Key': keys_collection.find_one({"_id":song["key_id"]})["secret"],
    }
    time.sleep(1)
    try:
        response = requests.get(f'https://hunt.spodown.com/api/v1/ext/downloads/{song["download_job_id"]}', headers=headers)
        response_json = json.loads(response.text)
        if response.status_code == 404:
            print(f"error when polling download {song_to_string(song)}")
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "errored"}})
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"error_message": response.text}})
            release_key(song)
            return None
        if response_json["status"] == "failed":
            print(f"error when polling download {song_to_string(song)}")
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "errored"}})
            songs_collection.update_one({"_id": song["_id"]}, {'$set': {"error_message": response.text}})
            release_key(song)
            return None
        if response_json["status"] == "downloading":
            print(f"downloading {song_to_string(song)}")
            return None
        if response_json["status"] == "ready":
            return response_json["download_url"]
    except Exception as e:
        print(f"error when polling download {song_to_string(song)}")
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "errored"}})
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"error_message": str(e)}})
        release_key(song)
        return None
    return None

def download_track(songs_collection:mongita.collection.Collection, keys_collection:mongita.collection.Collection, song, url):
    headers = {
        'X-Api-Key': keys_collection.find_one({"_id":song["key_id"]})["secret"],
    }
    try:
        filepath = get_file_path(song)
        time.sleep(1)
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            print("429 too many requests!")
            time.sleep(120)
            return 
        response.raise_for_status()
        directory = os.path.dirname(filepath)
        Path(directory).mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            f.write(response.content)
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "done"}})
        release_key(song)
        print(f"downloaded {song_to_string(song)}")
    except Exception as e:
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"status": "errored"}})
        songs_collection.update_one({"_id": song["_id"]}, {'$set': {"error_message": str(e)}})
        release_key(song)
        print(f"download error occured! {song_to_string(song)}")
        print(e)
    return

def format_filename(s):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    filename = ''.join(c for c in s if c in valid_chars)
    filename = filename.strip()
    return filename

if __name__ == '__main__':
    main()