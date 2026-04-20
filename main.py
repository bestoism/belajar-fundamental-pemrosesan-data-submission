import logging
from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import load_data

# Setup Logging agar rapi saat dijalankan
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Konfigurasi Kredensial yang sudah kamu buat
DB_URI = "postgresql://postgres:Dicoding-Pemda@db.zdqwrymmsvvtdcqddqad.supabase.co:5432/postgres"
SPREADSHEET_ID = "1pve_VhAfAodHI0u5530-cr8oLIrNa8m4Bih6yDlRfAo"

def run_pipeline():
    logging.info("=== Memulai Eksekusi ETL Pipeline ===")
    
    try:
        # TAHAP 1: EXTRACT
        # Mengambil dari 50 halaman (Total 1000 data sebelum dibersihkan, sesuai syarat Bintang 5)
        logging.info(">> TAHAP 1: EXTRACT DIMULAI")
        raw_data = extract_data(total_pages=50)
        
        # TAHAP 2: TRANSFORM
        logging.info(">> TAHAP 2: TRANSFORM DIMULAI")
        clean_data = transform_data(raw_data)
        
        # TAHAP 3: LOAD
        logging.info(">> TAHAP 3: LOAD DIMULAI")
        load_data(clean_data, db_uri=DB_URI, spreadsheet_id=SPREADSHEET_ID)
        
        logging.info("=== ETL Pipeline Berhasil Diselesaikan Tanpa Error! ===")
        
    except Exception as e:
        logging.error(f"=== ETL Pipeline Berhenti Karena Error: {e} ===")

if __name__ == "__main__":
    run_pipeline()