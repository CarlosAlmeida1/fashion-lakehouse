import os
import boto3
from pathlib import Path

BUCKET_NAME = "dominio-mercado-zara-project"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOCAL_FOLDER = DATA_DIR / "raw"

s3_client = boto3.client("s3")

def upload_files_to_s3(local_folder, bucket):
    if not local_folder.exists():
        print(f"ERRO: A pasta não existe: {local_folder}")
        return

    arquivos_encontrados = 0
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            if file.lower().endswith(".csv"):
                arquivos_encontrados += 1
                caminho_completo = Path(root) / file
                s3_key = caminho_completo.relative_to(DATA_DIR).as_posix()

                print(f"Uploading {caminho_completo.name} -> s3://{bucket}/{s3_key}")
                s3_client.upload_file(str(caminho_completo), bucket, s3_key)

    if arquivos_encontrados == 0:
        print(f"Aviso: A pasta {local_folder} foi localizada, mas nenhum arquivo .csv foi encontrado nela.")
    else:
        print(f"Total de {arquivos_encontrados} arquivos enviados com sucesso!")

if __name__ == "__main__":
    print(f"Buscando arquivos em: {LOCAL_FOLDER}")
    upload_files_to_s3(LOCAL_FOLDER, BUCKET_NAME)