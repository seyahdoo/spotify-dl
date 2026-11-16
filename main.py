"""Spotify playlist downloader and library synchronizer.

The worker ingests Spotify playlists into MongoDB, queues track downloads
through the spodown API, polls the download jobs, and writes finished tracks
to disk. A separate Flask dashboard (see ``flask-ui/``) reports progress.

Configuration is read from the environment. A ``secrets.env`` file is loaded
when present; see ``.env.example`` for the supported variables.

Usage:
    python main.py                       # run the download worker loop
    python main.py run --once            # run a single worker iteration
    python main.py ingest playlists.txt --owner sidal
    python main.py retry
    python main.py register-keys
    python main.py --help
"""

from __future__ import annotations

import argparse
import logging
import os
import pickle
import string
import time
from pathlib import Path
from typing import Any

import pymongo
import requests
import spotipy
from dotenv import load_dotenv
from pymongo.collection import Collection
from spotipy.oauth2 import SpotifyOAuth

load_dotenv("secrets.env")

LOGGER = logging.getLogger("spotidl")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DOWNLOADS_FOLDER = os.environ.get("DOWNLOADS_FOLDER", "/mnt/spotify-dl")
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DB_NAME = os.environ.get("MONGODB_DB_NAME", "db")

SPOTIFY_SCOPE = os.environ.get(
    "SPOTIFY_SCOPE",
    "user-library-read, playlist-read-private, playlist-modify-public",
)

SPODOWN_API_BASE_URL = os.environ.get(
    "SPODOWN_API_BASE_URL", "https://hunt.spodown.com/api/v1/ext"
).rstrip("/")
DOWNLOAD_FORMAT = os.environ.get("DOWNLOAD_FORMAT", "mp3-320")

MAX_DOWNLOADS_PER_KEY = int(os.environ.get("MAX_DOWNLOADS_PER_KEY", "2"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "60"))
RATE_LIMIT_BACKOFF_SECONDS = float(os.environ.get("RATE_LIMIT_BACKOFF_SECONDS", "120"))
MISSING_TRACKS_PLAYLIST_ID = os.environ.get("MISSING_TRACKS_PLAYLIST_ID")

# Up to three API keys are supported, matching SPODOWN_API_KEY_1..3.
SPODOWN_API_KEYS = [
    secret
    for secret in (os.environ.get(f"SPODOWN_API_KEY_{i}") for i in range(1, 4))
    if secret
]

# Song status values used across the collection.
STATUS_READY = "ready_to_start"
STATUS_DOWNLOADING = "downloading"
STATUS_DONE = "done"
STATUS_ERRORED = "errored"
STATUS_NOT_FOUND = "not_found"
STATUS_FOUND_IN_DISK = "found_in_disk"

SONG_STRING_FIELDS = (
    "owner",
    "playlist_name",
    "track_number",
    "song_name",
    "artist_name",
    "track_id",
)


# --------------------------------------------------------------------------- #
# Connections
# --------------------------------------------------------------------------- #

def connect_mongo() -> tuple[Collection, Collection]:
    """Connect to MongoDB and return the songs and keys collections."""
    client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
    except pymongo.errors.PyMongoError as exc:
        raise SystemExit(f"Could not connect to MongoDB: {exc}")
    database = client[MONGODB_DB_NAME]
    return database.songs_collection, database.keys_collection


def build_spotify_client() -> spotipy.Spotify:
    """Build an authenticated Spotipy client from the environment."""
    return spotipy.Spotify(auth_manager=SpotifyOAuth(scope=SPOTIFY_SCOPE))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def song_to_string(song: dict[str, Any]) -> str:
    return " - ".join(str(song.get(field, "")) for field in SONG_STRING_FIELDS)


def get_file_path(document: dict[str, Any]) -> str:
    return (
        f"{DOWNLOADS_FOLDER}/{document['owner']}/"
        f"{format_filename(document['playlist_name'])}/"
        f"{document['track_number']} - {format_filename(document['song_name'])}"
        f" - {format_filename(document['artist_name'])}.mp3"
    )


def get_track_folder(document: dict[str, Any]) -> str:
    return (
        f"{DOWNLOADS_FOLDER}/{document['owner']}/"
        f"{format_filename(document['playlist_name'])}"
    )


def format_filename(value: Any) -> str:
    """Sanitize a string so it is safe to use as a file or folder name."""
    valid_chars = f"-_.() {string.ascii_letters}{string.digits}"
    filename = "".join(c for c in str(value) if c in valid_chars).strip()
    filename = filename.rstrip(". ")
    return filename or "unknown"


def mark_song(
    songs_collection: Collection,
    song: dict[str, Any],
    status: str,
    error_message: str | None = None,
) -> None:
    """Update a song's status and, optionally, its last error message."""
    update: dict[str, Any] = {"status": status}
    if error_message is not None:
        update["error_message"] = error_message
    songs_collection.update_one({"_id": song["_id"]}, {"$set": update})


# --------------------------------------------------------------------------- #
# Key management
# --------------------------------------------------------------------------- #

def register_keys(keys_collection: Collection) -> None:
    """Register the configured spodown API keys (idempotent)."""
    if not SPODOWN_API_KEYS:
        raise SystemExit("No SPODOWN_API_KEY_* environment variables are set")
    for index, secret in enumerate(SPODOWN_API_KEYS, start=1):
        keys_collection.update_one(
            {"key_id": index},
            {
                "$set": {"secret": secret},
                "$setOnInsert": {"current_download_task_count": 0},
            },
            upsert=True,
        )
        LOGGER.info("Registered API key %d", index)


def release_key(
    songs_collection: Collection, keys_collection: Collection, song: dict[str, Any]
) -> None:
    """Release the download slot held by a finished song."""
    key_id = song.get("key_id")
    if key_id is None:
        return
    keys_collection.update_one(
        {"_id": key_id, "current_download_task_count": {"$gt": 0}},
        {"$inc": {"current_download_task_count": -1}},
    )
    songs_collection.update_one({"_id": song["_id"]}, {"$unset": {"key_id": 1}})


def reconcile_keys(songs_collection: Collection, keys_collection: Collection) -> None:
    """Rebuild per-key download counters from songs still in flight."""
    for key in keys_collection.find():
        in_flight = songs_collection.count_documents(
            {"status": STATUS_DOWNLOADING, "key_id": key["_id"]}
        )
        keys_collection.update_one(
            {"_id": key["_id"]},
            {"$set": {"current_download_task_count": in_flight}},
        )


def get_key_secret(keys_collection: Collection, song: dict[str, Any]) -> str | None:
    key = keys_collection.find_one({"_id": song.get("key_id")})
    return key["secret"] if key else None


# --------------------------------------------------------------------------- #
# Download queue
# --------------------------------------------------------------------------- #

def queue_update(songs_collection: Collection, keys_collection: Collection) -> None:
    for key in keys_collection.find():
        if key.get("current_download_task_count", 0) < MAX_DOWNLOADS_PER_KEY:
            queue_a_song_with_key(songs_collection, keys_collection, key)


def queue_a_song_with_key(
    songs_collection: Collection, keys_collection: Collection, key: dict[str, Any]
) -> None:
    song = songs_collection.find_one({"status": STATUS_READY})
    if song is not None:
        queue_download_track(songs_collection, keys_collection, song, key)


def queue_download_track(
    songs_collection: Collection,
    keys_collection: Collection,
    song: dict[str, Any],
    key: dict[str, Any],
) -> None:
    headers = {
        "X-Api-Key": key["secret"],
        "Content-Type": "application/json",
    }
    payload = {
        "spotify_url": f"https://open.spotify.com/track/{song['track_id']}",
        "format": DOWNLOAD_FORMAT,
    }

    try:
        response = requests.post(
            f"{SPODOWN_API_BASE_URL}/downloads",
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        LOGGER.warning("Failed to queue %s: %s", song_to_string(song), exc)
        return

    try:
        response_json = response.json()
    except ValueError:
        response_json = {}

    code = response_json.get("code")
    if code in {"track_unplayable", "track_not_found"}:
        mark_song(songs_collection, song, STATUS_NOT_FOUND, response.text)
        LOGGER.info("Track unavailable (%s): %s", code, song_to_string(song))
        return

    if response.status_code >= 400 or "job_id" not in response_json:
        LOGGER.warning(
            "Queue request failed (%s) for %s: %s",
            response.status_code,
            song_to_string(song),
            response.text,
        )
        return

    songs_collection.update_one(
        {"_id": song["_id"]},
        {
            "$set": {
                "download_job_id": response_json["job_id"],
                "status": STATUS_DOWNLOADING,
                "key_id": key["_id"],
            }
        },
    )
    keys_collection.update_one(
        {"_id": key["_id"]}, {"$inc": {"current_download_task_count": 1}}
    )
    LOGGER.info("Queued %s", song_to_string(song))


# --------------------------------------------------------------------------- #
# Download polling
# --------------------------------------------------------------------------- #

def poll_downloads(songs_collection: Collection, keys_collection: Collection) -> None:
    downloading_songs = list(songs_collection.find({"status": STATUS_DOWNLOADING}))
    for song in downloading_songs:
        url = check_download_ready(songs_collection, keys_collection, song)
        if url is not None:
            download_track(songs_collection, keys_collection, song, url)


def check_download_ready(
    songs_collection: Collection, keys_collection: Collection, song: dict[str, Any]
) -> str | None:
    secret = get_key_secret(keys_collection, song)
    if secret is None:
        LOGGER.warning("No API key for %s; releasing", song_to_string(song))
        mark_song(songs_collection, song, STATUS_ERRORED, "missing api key")
        release_key(songs_collection, keys_collection, song)
        return None

    headers = {"X-Api-Key": secret}
    try:
        response = requests.get(
            f"{SPODOWN_API_BASE_URL}/downloads/{song['download_job_id']}",
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        # Transient network problem: leave the song queued and retry later.
        LOGGER.warning("Poll failed for %s: %s", song_to_string(song), exc)
        return None

    if response.status_code == 404:
        mark_song(songs_collection, song, STATUS_ERRORED, response.text)
        release_key(songs_collection, keys_collection, song)
        LOGGER.warning("Poll returned 404 for %s", song_to_string(song))
        return None

    try:
        response_json = response.json()
    except ValueError:
        response_json = {}

    status = response_json.get("status")
    if status == "failed":
        mark_song(songs_collection, song, STATUS_ERRORED, response.text)
        release_key(songs_collection, keys_collection, song)
        LOGGER.warning("Download failed for %s", song_to_string(song))
        return None
    if status == "downloading":
        LOGGER.info("Downloading %s", song_to_string(song))
        return None
    if status == "ready":
        return response_json.get("download_url")

    LOGGER.warning("Unknown download status for %s: %s", song_to_string(song), status)
    return None


def download_track(
    songs_collection: Collection,
    keys_collection: Collection,
    song: dict[str, Any],
    url: str,
) -> None:
    secret = get_key_secret(keys_collection, song)
    headers = {"X-Api-Key": secret} if secret else {}
    filepath = get_file_path(song)

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
            stream=True,
        )
        if response.status_code == 429:
            LOGGER.warning(
                "Rate limited while downloading %s; retrying in %.0fs",
                song_to_string(song),
                RATE_LIMIT_BACKOFF_SECONDS,
            )
            time.sleep(RATE_LIMIT_BACKOFF_SECONDS)
            return
        response.raise_for_status()

        Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
        temp_path = f"{filepath}.part"
        with open(temp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=256 * 1024):
                handle.write(chunk)
        os.replace(temp_path, filepath)

        mark_song(songs_collection, song, STATUS_DONE)
        release_key(songs_collection, keys_collection, song)
        LOGGER.info("Downloaded %s", song_to_string(song))
    except Exception as exc:
        mark_song(songs_collection, song, STATUS_ERRORED, str(exc))
        release_key(songs_collection, keys_collection, song)
        LOGGER.exception("Download error for %s", song_to_string(song))


# --------------------------------------------------------------------------- #
# Playlist ingestion
# --------------------------------------------------------------------------- #

def process_txt(
    text_file_name: str,
    owner: str,
    songs_collection: Collection,
    spotipy_client: spotipy.Spotify,
    open_spotify_client: Any,
) -> None:
    with open(text_file_name, "r", encoding="utf-8") as handle:
        lines = handle.readlines()

    for line in lines:
        # - [ ] https://open.spotify.com/playlist/51NWUHnNVDi0VCm4dtuuhr
        if "/playlist/" not in line:
            continue
        playlist_id = line.split("/")[-1].split("?")[0].strip()
        if not playlist_id:
            continue
        LOGGER.info("Ingesting playlist %s", playlist_id)
        try:
            ingest_track_ids_open_spotify(
                owner, songs_collection, open_spotify_client, playlist_id
            )
        except Exception:
            LOGGER.warning(
                "Open Spotify scrape failed for %s; falling back to the Spotify API",
                playlist_id,
            )
            ingest_track_ids_spotipy(owner, songs_collection, spotipy_client, playlist_id)


def ingest_track_ids_spotipy(
    owner: str,
    songs_collection: Collection,
    spotipy_client: spotipy.Spotify,
    playlist_id: str,
) -> None:
    time.sleep(0.5)
    playlist_data = spotipy_client.playlist(playlist_id)
    playlist_name = playlist_data["name"]
    LOGGER.info("Ingesting via Spotipy: %s", playlist_name)

    track_number = 0
    results = spotipy_client.playlist_items(playlist_id, market="TR")
    while results:
        for item in results["items"]:
            track = item.get("item") or item.get("track")
            if not track or not track.get("id"):
                continue
            track_number += 1
            document = {
                "owner": owner,
                "track_id": track["id"],
                "playlist_name": playlist_name,
                "song_name": track["name"],
                "artist_name": track["artists"][0]["name"],
                "track_number": track_number,
                "status": STATUS_READY,
                "download_url": "",
                "download_job_id": "",
                "source": "spotipy",
            }
            add_song_to_collection(songs_collection, document)
        if results.get("next"):
            time.sleep(0.5)
            results = spotipy_client.next(results)
        else:
            break


def ingest_track_ids_open_spotify(
    owner: str,
    songs_collection: Collection,
    open_spotify_client: Any,
    playlist_id: str,
) -> None:
    time.sleep(0.5)
    playlist = open_spotify_client.get_playlist(
        f"https://open.spotify.com/playlist/{playlist_id}", max_tracks=10000
    )
    playlist_name = playlist.name
    LOGGER.info("Ingesting via open Spotify: %s", playlist_name)

    for index, track in enumerate(playlist.tracks):
        document = {
            "owner": owner,
            "track_id": track.track.id,
            "playlist_name": playlist_name,
            "song_name": track.track.name,
            "artist_name": track.track.artists[0].name,
            "track_number": index + 1,
            "status": STATUS_READY,
            "download_url": "",
            "download_job_id": "",
            "source": "open_spotify",
        }
        add_song_to_collection(songs_collection, document)


def add_song_to_collection(songs_collection: Collection, song: dict[str, Any]) -> None:
    existing = songs_collection.find_one(
        {
            "owner": song["owner"],
            "playlist_name": song["playlist_name"],
            "track_id": song["track_id"],
        }
    )
    if existing is not None:
        return

    songs_collection.insert_one(song)
    LOGGER.info("Added song: %s", song_to_string(song))


def process_songs_to_be_ingested(
    text_file_name: str, owner: str, songs_collection: Collection
) -> None:
    # Imported lazily: the scraper pulls in a browser stack that the worker
    # loop does not need.
    from spotify_scraper import SpotifyClient

    spotipy_client = build_spotify_client()
    with SpotifyClient() as open_spotify_client:
        process_txt(
            text_file_name,
            owner,
            songs_collection,
            spotipy_client,
            open_spotify_client,
        )


# --------------------------------------------------------------------------- #
# Maintenance commands
# --------------------------------------------------------------------------- #

def retry_all(songs_collection: Collection) -> None:
    for status in (STATUS_ERRORED, STATUS_NOT_FOUND):
        result = songs_collection.update_many(
            {"status": status}, {"$set": {"status": STATUS_READY}}
        )
        LOGGER.info("Requeued %d %s tracks", result.modified_count, status)


def add_all_not_found_songs_to_a_playlist(songs_collection: Collection) -> None:
    if not MISSING_TRACKS_PLAYLIST_ID:
        raise SystemExit(
            "MISSING_TRACKS_PLAYLIST_ID must be set to use this command"
        )

    spotipy_client = build_spotify_client()
    track_ids = [
        song["track_id"]
        for song in songs_collection.find({"status": STATUS_NOT_FOUND})
        if song.get("track_id")
    ]
    for start in range(0, len(track_ids), 100):
        spotipy_client.playlist_add_items(
            MISSING_TRACKS_PLAYLIST_ID, track_ids[start : start + 100]
        )
    LOGGER.info("Added %d missing tracks to the playlist", len(track_ids))


def find_tracks_on_folders(songs_collection: Collection) -> None:
    """Mark not-found tracks as done when a matching file exists on disk."""
    songs = songs_collection.find({"status": STATUS_NOT_FOUND})
    for song in songs:
        folder = get_track_folder(song)
        track_number = str(song["track_number"])
        try:
            files = os.listdir(folder)
        except OSError:
            continue
        if any(name.startswith(track_number) for name in files):
            mark_song(songs_collection, song, STATUS_DONE)
            LOGGER.info("Found on disk: %s", song_to_string(song))


def move_missing_tracks_to_ready_to_start(songs_collection: Collection) -> None:
    result = songs_collection.update_many(
        {"status": STATUS_FOUND_IN_DISK}, {"$set": {"status": STATUS_DONE}}
    )
    LOGGER.info("Marked %d found-in-disk tracks as done", result.modified_count)


def get_songs_from_found_folder(
    songs_collection: Collection, attributes_path: str
) -> None:
    """Re-file tracks from the ``found`` folder using pickled Windows metadata."""
    found_folder = os.path.join(DOWNLOADS_FOLDER, "found")
    with open(attributes_path, "rb") as handle:
        attributes = pickle.load(handle)

    for filename in os.listdir(found_folder):
        file_path = os.path.join(found_folder, filename)
        attr = attributes.get(file_path)
        if attr is None:
            continue
        file_title = attr.get("Title")
        query = {"status": STATUS_NOT_FOUND, "song_name": file_title}
        if songs_collection.count_documents(query) != 1:
            LOGGER.warning("Could not uniquely match found file: %s", file_path)
            continue
        song = songs_collection.find_one(query)
        supposed_path = get_file_path(song)
        Path(os.path.dirname(supposed_path)).mkdir(parents=True, exist_ok=True)
        os.replace(file_path, supposed_path)
        LOGGER.info("Re-filed %s -> %s", file_path, supposed_path)


def update_playlists() -> None:
    """Write a ``<folder>.m3u`` playlist into every folder that holds mp3s."""
    for root, _dirs, files in os.walk(DOWNLOADS_FOLDER):
        tracks = [name for name in files if name.endswith(".mp3")]
        if not tracks:
            continue
        folder_name = os.path.basename(os.path.normpath(root))
        tracks.append(f"{folder_name}.m3u")
        playlist_file_path = os.path.join(root, f"{folder_name}.m3u")
        with open(playlist_file_path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(tracks) + "\n")


# --------------------------------------------------------------------------- #
# Worker loop
# --------------------------------------------------------------------------- #

def run_worker(
    songs_collection: Collection, keys_collection: Collection, once: bool = False
) -> None:
    reconcile_keys(songs_collection, keys_collection)
    LOGGER.info("Worker started (poll interval %.1fs)", POLL_INTERVAL_SECONDS)
    try:
        while True:
            try:
                queue_update(songs_collection, keys_collection)
                poll_downloads(songs_collection, keys_collection)
                update_playlists()
            except Exception:
                LOGGER.exception("Worker iteration failed; continuing")
            if once:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        LOGGER.info("Worker stopped")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level (default: %(default)s)",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run the download worker loop (default).")
    run_parser.add_argument(
        "--once", action="store_true", help="Run a single iteration and exit."
    )

    ingest_parser = subparsers.add_parser(
        "ingest", help="Ingest playlists from a text file."
    )
    ingest_parser.add_argument(
        "text_file", nargs="?", default="playlists-sidal.txt",
        help="Text file containing Spotify playlist URLs.",
    )
    ingest_parser.add_argument("--owner", default="sidal", help="Owner label for the tracks.")

    subparsers.add_parser("register-keys", help="Register the configured API keys.")
    subparsers.add_parser("retry", help="Requeue errored and not-found tracks.")
    subparsers.add_parser(
        "scan-folders", help="Mark not-found tracks that exist on disk as done."
    )
    subparsers.add_parser(
        "mark-done", help="Mark found-in-disk tracks as done."
    )
    subparsers.add_parser(
        "add-missing-to-playlist", help="Add not-found tracks to the missing playlist."
    )
    subparsers.add_parser(
        "update-playlists", help="Regenerate the .m3u playlist files on disk."
    )
    sync_parser = subparsers.add_parser(
        "sync-found", help="Re-file tracks from the found folder."
    )
    sync_parser.add_argument(
        "--attributes", default="attributes.pkl", help="Pickled Windows metadata file."
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    command = args.command or "run"

    if command == "update-playlists":
        update_playlists()
        return 0

    if command == "ingest":
        songs_collection, _ = connect_mongo()
        process_songs_to_be_ingested(args.text_file, args.owner, songs_collection)
        return 0

    songs_collection, keys_collection = connect_mongo()

    if command == "register-keys":
        register_keys(keys_collection)
    elif command == "retry":
        retry_all(songs_collection)
    elif command == "scan-folders":
        find_tracks_on_folders(songs_collection)
    elif command == "mark-done":
        move_missing_tracks_to_ready_to_start(songs_collection)
    elif command == "add-missing-to-playlist":
        add_all_not_found_songs_to_a_playlist(songs_collection)
    elif command == "sync-found":
        get_songs_from_found_folder(songs_collection, args.attributes)
    else:
        run_worker(songs_collection, keys_collection, once=getattr(args, "once", False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
