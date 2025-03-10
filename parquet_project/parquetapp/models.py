import pandas as pd
import os


class ParquetModel():
    """
    Modèle inspiré de Django pour interagir
    avec des fichiers Parquet comme une base de données.
    """
    def __init__(self, file_path=None):
        self.file_path = file_path

    def load_data(self):
        """Charge les données du fichier Parquet."""
        if os.path.exists(self.file_path):
            return pd.read_parquet(self.file_path)
        return pd.DataFrame()

    def save_data(self, df):
        """Sauvegarde les données dans un fichier Parquet."""
        df.to_parquet(self.file_path, index=False)

    def all(self):
        """Retourne toutes les données sous forme de DataFrame."""
        return self.load_data()

    def filter(self, **kwargs):
        """Filtre les données selon les critères fournis."""
        df = self.load_data()
        for key, value in kwargs.items():
            df = df[df[key] == value]
        return df

    def create(self, **kwargs):
        """Crée un nouvel enregistrement."""
        df = self.load_data()
        new_entry = pd.DataFrame([kwargs])
        df = pd.concat([df, new_entry], ignore_index=True)
        self.save_data(df)
        return new_entry

    def update(self, filter_kwargs, update_kwargs):
        """Met à jour un enregistrement existant."""
        df = self.load_data()
        for key, value in filter_kwargs.items():
            df.loc[
                df[key] == value, list(update_kwargs.keys())
            ] = list(update_kwargs.values())
        self.save_data(df)
        return df

    def delete(self, **kwargs):
        """Supprime un enregistrement."""
        df = self.load_data()
        for key, value in kwargs.items():
            df = df[df[key] != value]
        self.save_data(df)
        return df
