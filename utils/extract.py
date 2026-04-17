import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def extract_data(base_url="https://fashion-studio.dicoding.dev", total_pages=50):
    """
    Mengekstrak data produk dari website menggunakan requests dan BeautifulSoup.
    """
    all_products = []
    
    # Menggunakan Session untuk efisiensi saat scraping banyak halaman
    session = requests.Session()
    
    # Menambahkan timestamp saat fungsi dipanggil (Syarat Skilled - 3 pts)
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"Memulai ekstraksi data dari {base_url} (Total Halaman: {total_pages})")
    
    for page in range(1, total_pages + 1):
        url = f"{base_url}/?page={page}"
        
        try: # Syarat Advanced (4 pts) - Error handling di fungsi
            response = session.get(url, timeout=10)
            response.raise_for_status() 
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Asumsi elemen pembungkus produk memiliki class 'card'
            product_cards = soup.find_all('div', class_='card')
            
            for card in product_cards:
                try:
                    title_elem = card.find('h5', class_='card-title')
                    title = title_elem.text.strip() if title_elem else "Unknown Product"
                    
                    # Mencari Price (handle $xxx atau Price Unavailable)
                    price_elem = card.find('p', class_='price') or card.find('span', class_='price')
                    price = price_elem.text.strip() if price_elem else "Price Unavailable"
                    
                    rating_elem = card.find('span', class_='rating')
                    rating = rating_elem.text.strip() if rating_elem else "Invalid Rating"
                    
                    color_elem = card.find('span', class_='color')
                    colors = color_elem.text.strip() if color_elem else "Unknown"
                    
                    size_elem = card.find('span', class_='size')
                    size = size_elem.text.strip() if size_elem else "Size: Unknown"
                    
                    gender_elem = card.find('span', class_='gender')
                    gender = gender_elem.text.strip() if gender_elem else "Gender: Unknown"
                    
                    all_products.append({
                        "Title": title,
                        "Price": price,
                        "Rating": rating,
                        "Colors": colors,
                        "Size": size,
                        "Gender": gender,
                        "timestamp": current_timestamp
                    })
                except Exception as inner_e:
                    logging.warning(f"Gagal memparsing produk di halaman {page}: {inner_e}")
                    continue
                    
        except requests.exceptions.RequestException as e:
            logging.error(f"Error saat mengakses halaman {page}: {e}")
            continue # Lanjut ke halaman berikutnya jika halaman ini error
            
    df_extracted = pd.DataFrame(all_products)
    logging.info(f"Ekstraksi selesai. Total data: {len(df_extracted)}")
    
    return df_extracted

if __name__ == "__main__":
    # Test script untuk 2 halaman pertama
    df = extract_data(total_pages=2)
    print(df.head())