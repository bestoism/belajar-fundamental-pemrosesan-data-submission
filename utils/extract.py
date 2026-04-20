import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import re

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def extract_data(base_url="https://fashion-studio.dicoding.dev", total_pages=50):
    """
    Mengekstrak data produk dari website menggunakan Regex & BeautifulSoup yang tangguh.
    """
    all_products =[]
    session = requests.Session()
    
    # Syarat 3 pts: timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    logging.info(f"Memulai ekstraksi dari {base_url} (Total Halaman: {total_pages})")
    
    for page in range(1, total_pages + 1):
        # Memperbaiki penanganan Pagination (Halaman 1 vs Halaman 2 dst)
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}/page{page}"
            
        try: # Syarat 4 pts: Error Handling
            response = session.get(url, timeout=10)
            response.raise_for_status() 
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # MENGGUNAKAN CLASS YANG BENAR (Berdasarkan hasil debug)
            product_cards = soup.find_all('div', class_='collection-card')
            
            for card in product_cards:
                try:
                    # Mengambil semua teks dalam card dan memisahkannya dengan '|'
                    # (Cara paling aman untuk mengabaikan tag HTML yang berubah-ubah)
                    full_text = card.get_text(separator=' | ', strip=True)
                    
                    # 1. Mencari Title (Biasanya di tag H)
                    title_elem = card.find(['h2', 'h3', 'h4', 'h5'])
                    title = title_elem.text.strip() if title_elem else "Unknown Product"
                    
                    # 2. Mencari Price (Dalam price-container)
                    price_container = card.find('div', class_='price-container')
                    price = price_container.text.strip() if price_container else "Price Unavailable"
                    
                    # 3. Mencari Rating dengan Regex
                    rating_match = re.search(r'Rating:\s*([^|]+)', full_text)
                    rating = rating_match.group(1).replace('⭐', '').strip() if rating_match else "Invalid Rating"
                    
                    # 4. Mencari Colors
                    colors_match = re.search(r'(\d+\s*Colors?)', full_text)
                    colors = colors_match.group(1).strip() if colors_match else "Unknown"
                    
                    # 5. Mencari Size
                    size_match = re.search(r'Size:\s*([^|]+)', full_text)
                    size = f"Size: {size_match.group(1).strip()}" if size_match else "Size: Unknown"
                    
                    # 6. Mencari Gender
                    gender_match = re.search(r'Gender:\s*([^|]+)', full_text)
                    gender = f"Gender: {gender_match.group(1).strip()}" if gender_match else "Gender: Unknown"
                    
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
                    logging.warning(f"Gagal memparsing 1 produk: {inner_e}")
                    continue
                    
        except requests.exceptions.RequestException as e:
            logging.error(f"Error saat mengakses halaman {page}: {e}")
            continue
            
    # Mencegah KeyError jika internet putus / gagal scraping
    if not all_products:
        df_extracted = pd.DataFrame(columns=["Title", "Price", "Rating", "Colors", "Size", "Gender", "timestamp"])
    else:
        df_extracted = pd.DataFrame(all_products)
        
    logging.info(f"Ekstraksi selesai. Total data: {len(df_extracted)}")
    
    return df_extracted

if __name__ == "__main__":
    df = extract_data(total_pages=2)
    print(df.head())