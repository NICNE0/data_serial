import pandas as pd
import os
from io import StringIO

def serialize(data: pd.DataFrame, name: str) -> None:
    try:
        buffer = StringIO()
        
        for row in data.itertuples(index=False):
            buffer.write("---\n")
            row_dict = row._asdict()
            for column, value in row_dict.items():
                buffer.write(f"{column}: {value}\n")

        file_path = f"{name}.yml"

        os.makedirs(os.path.dirname(file_path), exist_ok=True) if os.path.dirname(file_path) else None

        with open(file_path, 'w') as f:
            f.write(buffer.getvalue())

    except Exception as e:
        print(f"Error serializing DataFrame to YAML: {e}")


def deserialize(file_path: str) -> pd.DataFrame:
    try:
        with open(file_path, 'r') as f:
            content = f.read()

        blocks = content.strip().split('---\n')
        records = []

        for block in blocks:
            if not block.strip():
                continue
            row = {}
            for line in block.strip().split('\n'):
                if ": " in line:
                    key, value = line.split(": ", 1)
                    row[key] = value
            records.append(row)

        return pd.DataFrame(records)

    except Exception as e:
        print(f"Error deserializing YAML to DataFrame: {e}")
        return pd.DataFrame()