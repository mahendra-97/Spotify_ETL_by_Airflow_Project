import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
import pandas as pd
from datetime import datetime
# from azure.storage.blob import BlobServiceClient
import s3fs 


def convert_ms_to_min_sec(duration_ms):
    duration_sec = duration_ms / 1000
    minutes, seconds = divmod(duration_sec, 60)
    return (f"{int(minutes)}min {int(seconds)}sec")

def spotify_data_extract():
    client_id = 'd373fb6560d24ad5a0cfed75ae7c1e64'
    client_secret = '4b6692379740491fbabfcaf5979667f1'

    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    artist_id = '1mYsTxnqsietFxj1OgoGbG'
    top_tracks = sp.artist_top_tracks(artist_id)
    return top_tracks

def spotify_data_transform(**kwargs):
    ti = kwargs['ti']
    top_tracks = ti.xcom_pull(task_ids='extract_spotify_data')

    tracks_data = []

    for track in top_tracks["tracks"]:
        track_info = {
            "album_type": track["album"]["album_type"],
            # "album_id": track["album"]["id"],
            "album_name": track["album"]["name"],
            "release_date": track["album"]["release_date"],
            "total_tracks": track["album"]["total_tracks"],
            "artist_names": ', '.join(artist["name"] for artist in track["artists"]),
            # "track_id": track["id"],
            "track_name": track["name"],
            "duration_ms": convert_ms_to_min_sec(track["duration_ms"]),
            "popularity": track["popularity"]
            # "preview_url": track["preview_url"],
        }
        tracks_data.append(track_info)

    df_tracks = pd.DataFrame(tracks_data)

    return df_tracks

def spotify_data_load(**kwargs):
    ti = kwargs['ti']
    df_tracks = ti.xcom_pull(task_ids='transform_spotify_data')
    df_tracks.to_csv("s3://spotify-airflow-project/toptracksdata.csv", index=False)

    # # Replace your Azure Storage account details
    # azure_account_name = 'spotifyairflow'
    # azure_account_key = 'azure_account_key'
    # azure_container = 'spotify-data'
    # azure_blob_path = 'toptracksdata.csv'

    # blob_service_client = BlobServiceClient(account_url=f"https://{azure_account_name}.blob.core.windows.net", credential=azure_account_key)
    # blob_client = blob_service_client.get_blob_client(container=azure_container, blob=azure_blob_path)

    # csv_data = df_tracks.to_csv(index=False)
    # blob_client.upload_blob(csv_data, overwrite=True)

