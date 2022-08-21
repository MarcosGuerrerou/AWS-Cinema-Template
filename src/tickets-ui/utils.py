import requests
import pandas as pd

class TMDB(object):
    def __init__(self):
        from decouple import config, UndefinedValueError

        self.url = "https://api.themoviedb.org"
        self.img_url = "https://image.tmdb.org/t/p"
        
        try:
            self.api_key = config('TMDB')
        except UndefinedValueError:
            raise ValueError('TMDB api key is missing, provide it as an environment variable')
    
    def get_movies_playing(self, language: str = 'en-US', region: str = 'US', include_images: bool = False, n_newest: int = 10) -> pd.DataFrame:

        response = requests.get(
            url = f'{self.url}/3/movie/now_playing/',
            params = {'language': language, 'region': region, 'api_key': self.api_key}
        )

        if response.ok:
            self.df = pd.DataFrame(response.json()['results']).sort_values(by = 'release_date', ascending = False).reset_index().drop('index', axis = 1)
           
            if n_newest < 0:
                raise ValueError('The value of the "n_newest" parameter must be positive')
            elif n_newest > 0 and n_newest < len(self.df):
                last_idx = 1 if n_newest == 1 else n_newest-1
                self.df = self.df[0:last_idx]
            
            if include_images:
                return self.attach_images(df = self.df, include_url = True)
            else:
                return self.df
        else:
            raise Exception(response.text)
    
    def get_movies_upcoming(self, language: str = 'en-US', region: str = 'US', include_images: bool = False, n_newest: int = 10) -> pd.DataFrame:

        response = requests.get(
            url = f'{self.url}/3/movie/upcoming/',
            params = {'language': language, 'region': region, 'api_key': self.api_key}
        )

        if response.ok:
            self.df_up = pd.DataFrame(response.json()['results']).sort_values(by = 'release_date', ascending = True).reset_index().drop('index', axis = 1)
            
            if n_newest < 0:
                raise ValueError('The value of the "n_newest" parameter must be positive')
            elif n_newest > 0 and n_newest < len(self.df_up):
                last_idx = 1 if n_newest == 1 else n_newest-1
                self.df_up = self.df_up[0:last_idx]
            
            if include_images:
                return self.attach_images(df = self.df_up, include_url = True)
            else:
                return self.df_up
        else:
            raise Exception(response.text)
    

    def attach_images(self, df: pd.DataFrame, include_url: bool, include_bytes: bool = False) -> pd.DataFrame:

        if include_url:
            df['img_url'] = df.apply(lambda film: f"{self.img_url}/w500{film['poster_path']}", axis=1)

        if include_bytes:
            df['img_bytes'] = df.apply(lambda film: TMDB.get_image(f"{self.img_url}/w500{film['poster_path']}"), axis=1)
        
        return df

    @staticmethod
    def get_image(path: str):
        response = requests.get(
            url = path
        )
        return response.content