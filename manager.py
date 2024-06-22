import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm


class SpotifyManager():

    def __init__(self, scope: str = 'user-library-read', limit: int = 50):
        self.spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
        self.limit = limit

    def backup(self):
        return self._get_tracks()

    @property
    def total_songs(self):
        return self.spotify.current_user_saved_tracks()['total']

    def _build_offset_range(self, n_songs: int):
        return list(range(0, self.total_songs, self.limit))

    def _get_tracks(self) -> pd.DataFrame:
        offsets = self._build_offset_range(n_songs=self.total_songs)
        data: list[dict] = []
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(self._get_tracks_batch, offset) for offset in offsets]
            for future in tqdm(futures, ncols=80):
                data.extend(future.result())
        return pd.DataFrame.from_dict(data)

    def _get_tracks_batch(self, offset: int) -> list[dict]:
        batch_tracks = []
        results = self.spotify.current_user_saved_tracks(limit=self.limit, offset=offset)

        if not results:
            return []

        for item in results['items']:
            batch_tracks.append(self._extract_data_from_request(request_response=item))

        return batch_tracks

    def _extract_data_from_request(self, request_response: dict) -> dict:
        track = request_response['track']
        return {
            'song_name': track['name'],
            'artist_name': track['artists'][0]['name'],
            'algum_name': track['album']['name'],
            'album_release_date': track['album']['release_date'],
            'disc_number': track['disc_number'],
            'track_number': track['track_number'],
            'added_at': request_response['added_at'],
            'spotify_uri': track['uri'],
        }
