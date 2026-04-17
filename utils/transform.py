import pandas as pd
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def transform_data(df):
    """
    Membersihkan dan mentransformasi DataFrame hasil ekstraksi.
    """
    logging.info("Memulai proses transformasi data...")
    
    try: # Syarat Advanced (4 pts) - Error Handling
        # 1. Menghapus data duplikat dan baris yang kosong (null)
        df = df.drop_duplicates()
        df = df.dropna()
        
        # 2. Memfilter data invalid sesuai instruksi rubrik
        df = df[df['Title'] != 'Unknown Product']
        df = df[df['Rating'] != 'Invalid Rating']
        
        # Menghapus harga yang "Price Unavailable" 
        # (Menggunakan string contains untuk memastikan tidak ada variasi spasi)
        df = df[~df['Price'].astype(str).str.contains('Price Unavailable', case=False, na=False)]
        
        # 3. Transformasi Kolom 'Price'
        # Hapus simbol $, ubah ke numeric, kalikan 16000
        df['Price'] = df['Price'].str.replace('$', '', regex=False).str.strip()
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce') * 16000
        
        # 4. Transformasi Kolom 'Rating'
        # Hapus teks " / 5", ubah ke float
        df['Rating'] = df['Rating'].str.replace(' / 5', '', regex=False).str.strip()
        df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
        
        # 5. Transformasi Kolom 'Colors'
        # Menggunakan str.extract() untuk mengambil angkanya saja (sesuai Tips & Tricks)
        df['Colors'] = df['Colors'].astype(str).str.extract(r'(\d+)')[0]
        df['Colors'] = pd.to_numeric(df['Colors'], errors='coerce')
        
        # 6. Transformasi Kolom 'Size'
        # Hapus teks "Size: "
        df['Size'] = df['Size'].str.replace('Size:', '', regex=False).str.strip()
        
        # 7. Transformasi Kolom 'Gender'
        # Hapus teks "Gender: "
        df['Gender'] = df['Gender'].str.replace('Gender:', '', regex=False).str.strip()
        
        # 8. Hapus null sekali lagi (jika ada data gagal di-convert menjadi NaN)
        df = df.dropna()
        
        # 9. Pastikan tipe data persis seperti ekspektasi rubrik Basic
        df['Price'] = df['Price'].astype('int64')
        df['Rating'] = df['Rating'].astype('float64')
        df['Colors'] = df['Colors'].astype('int64')
        
        # Kolom timestamp, Size, Gender, Title biarkan sebagai string/object
        
        logging.info("Transformasi selesai. Kualitas data sudah siap pakai.")
        return df

    except Exception as e:
        logging.error(f"Terjadi kesalahan saat transformasi data: {e}")
        # Lemparkan error ke atas agar pipeline tahu proses gagal
        raise e

# Blok ini untuk test fungsi secara langsung
if __name__ == "__main__":
    # Membuat dummy data yang kotor
    raw_data = pd.DataFrame({
        "Title": ["T-Shirt Keren", "Unknown Product", "Jacket Kece"],
        "Price": ["$20", "Price Unavailable", "$50.5"],
        "Rating": ["4.8 / 5", "Invalid Rating", "4.0 / 5"],
        "Colors": ["3 Colors", "Unknown", "1 Color"],
        "Size": ["Size: M", "Size: L", "Size: XL"],
        "Gender": ["Gender: Men", "Gender: Women", "Gender: Unisex"],
        "timestamp": ["2023-11-01", "2023-11-01", "2023-11-01"]
    })
    
    print("=== DATA SEBELUM ===")
    print(raw_data)
    
    print("\n=== DATA SESUDAH ===")
    clean_df = transform_data(raw_data)
    print(clean_df)
    print("\n=== TIPE DATA ===")
    print(clean_df.dtypes)