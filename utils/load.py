import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def load_to_csv(df, filename="products.csv"):
    try:
        df.to_csv(filename, index=False)
        logging.info(f"Berhasil menyimpan data ke CSV: {filename}")
    except Exception as e:
        logging.error(f"Gagal menyimpan ke CSV: {e}")
        raise e

def load_to_postgres(df, db_uri):
    try:
        # Menggunakan SQLAlchemy untuk koneksi ke PostgreSQL
        engine = create_engine(db_uri)
        # Menyimpan DataFrame ke tabel 'competitor_products'
        df.to_sql('competitor_products', engine, if_exists='replace', index=False)
        logging.info("Berhasil menyimpan data ke PostgreSQL.")
    except Exception as e:
        logging.error(f"Gagal menyimpan ke PostgreSQL: {e}")
        raise e

def load_to_gsheets(df, spreadsheet_id, credentials_file="google-sheets-api.json"):
    try:
        # Cek apakah file kredensial ada
        if not os.path.exists(credentials_file):
            logging.warning(f"File kredensial {credentials_file} tidak ditemukan. Skip Google Sheets.")
            return

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)

        # Ubah DataFrame menjadi bentuk List of Lists agar bisa dibaca Google Sheets
        # Memasukkan header (nama kolom) di baris pertama
        data_values = [df.columns.values.tolist()] + df.values.tolist()
        
        body = {'values': data_values}
        
        # Menyimpan ke Sheet1 mulai dari sel A1
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="RAW",
            body=body
        ).execute()
        
        logging.info(f"Berhasil menyimpan {result.get('updatedCells')} sel ke Google Sheets.")
    except Exception as e:
        logging.error(f"Gagal menyimpan ke Google Sheets: {e}")
        raise e

def load_data(df, db_uri=None, spreadsheet_id=None):
    """
    Fungsi utama untuk menjalankan semua proses loading.
    """
    logging.info("Memulai proses load data...")
    
    # 1. Load ke CSV
    load_to_csv(df)
    
    # 2. Load ke PostgreSQL (Jika URI disediakan)
    if db_uri:
        load_to_postgres(df, db_uri)
    else:
        logging.warning("DB_URI tidak disediakan. Skip PostgreSQL.")
        
    # 3. Load ke Google Sheets (Jika ID disediakan)
    if spreadsheet_id:
        load_to_gsheets(df, spreadsheet_id)
    else:
        logging.warning("Spreadsheet ID tidak disediakan. Skip Google Sheets.")
        
    logging.info("Proses load data selesai.")

# Blok testing
if __name__ == "__main__":
    dummy_df = pd.DataFrame({"Title": ["Test"], "Price": [100000]})
    # Hanya test CSV untuk sementara agar tidak error saat koneksi DB belum ada
    load_to_csv(dummy_df, "test_products.csv")