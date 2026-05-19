from constants import path_strings
from data_quality import gx
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
import os
import src.extraction as ext
import src.load as ld
import shutil
# import src.transformation as trf
import urllib.parse

load_dotenv()


def clean_gx_metadata():
    """Remove pastas de metadados do GX para garantir um fresh start."""
    # O caminho deve ser o mesmo que o GX usa no seu Docker (/app/gx)
    gx_folder = Path("/app/gx")
    
    if gx_folder.exists() and gx_folder.is_dir():
        print(f"Cleaning GX metadata at {gx_folder}...")
        try:
            # Remove a pasta e tudo dentro dela
            shutil.rmtree(gx_folder)
            # Recria a pasta vazia para o GX não reclamar que o diretório sumiu
            gx_folder.mkdir(parents=True, exist_ok=True)
            print("GX metadata cleaned successfully.")
        except Exception as e:
            print(f"Warning: Could not clean GX folder: {e}")

def main():

    # clean_gx_metadata()

    # Path and requirements

    url = path_strings.url_main
    url_metadata = path_strings.metadata_url
    bronze_path = Path(path_strings.bronze_path)
    mounted_gx_location = path_strings.gx_location
    # Caminho para o index gerado pelo GX dentro do volume montado
    gx_index_file = mounted_gx_location / "uncommitted" / "data_docs" / "local_site" / "index.html"
    raw_main_path = Path(path_strings.raw_main_path)

    # ------------------ Extraction using commit a499dd34c1372468f2335a370c5dd13cc3a72d90

    if not any(bronze_path.iterdir()):
        print("Starting extraction ...")
        ext.extract(url, url_metadata)
    else:
        print("Data already available. Skipping extraction ...")
    
    # Validation Layer on raw data

    gx.run_validation()


    # ------------------- Load to Postgres (Before transformation)

    raw_parquet = ld.load_silver(raw_main_path)  # Load .parquet raw dataframe

    # 2. Get .env endpoints

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # 3. Encode the password to handle special characters (@, !, #, etc...)
    encoded_password = urllib.parse.quote_plus(password)

    # 4. Create the engine to connect to Postgres Database

    engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}")

    # 5. Push raw directly to Postgres
    ld.push_to_db(raw_parquet, "raw_full", engine, schema="staging_raw")


if __name__ == "__main__":
    main()