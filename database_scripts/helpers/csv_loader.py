import pandas as pd

def load_csv(name, limit=None):
    path = f"../data_generation/generated_data/{name}.csv"
    df = pd.read_csv(path)
    if limit:
        return df.head(limit)
    return df
