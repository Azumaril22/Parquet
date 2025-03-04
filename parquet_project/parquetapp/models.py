import pandas as pd

class ParquetUser:
    parquet_file = "parquetapp/data/users.parquet"

    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email

    @classmethod
    def all(cls):
        try:
            df = pd.read_parquet(cls.parquet_file)
            return [cls(**row.to_dict()) for _, row in df.iterrows()]
        except FileNotFoundError:
            return []

    @classmethod
    def filter(cls, **conditions):
        df = pd.read_parquet(cls.parquet_file)
        for key, value in conditions.items():
            df = df[df[key] == value]
        return [cls(**row.to_dict()) for _, row in df.iterrows()]

    @classmethod
    def save(cls, user_obj):
        try:
            df = pd.read_parquet(cls.parquet_file)
        except FileNotFoundError:
            df = pd.DataFrame(columns=['id', 'username', 'email'])

        df = df[df['id'] != user_obj.id]
        new_row = pd.DataFrame([user_obj.__dict__])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_parquet(cls.parquet_file)

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
        }
