import requests
import pandas as pd
import re
from datetime import datetime
import os
import argparse


def fetch_cme_text(year: int, month: int) -> str:
    """
    Fetch the CME catalog text for a given year and month from the SIDC website.
    """
    base_url = "https://www.sidc.be/cactus/catalog/LASCO/2_5_0/qkl"
    url = f"{base_url}/{year}/{month:02d}/cmecat.txt"
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def parse_cme_data_from_text(text_data: str) -> pd.DataFrame:
    lines = text_data.splitlines()
    data_lines = []

    for line in lines:
        line = line.strip()
        if line.startswith('# Flow|'):
            break  # Stop parsing when Flow section begins
        if re.match(r'^\d{4}\|', line):
            data_lines.append(line)

    records = []
    for line in data_lines:
        parts = [p.strip() for p in line.split('|')]
        # Expect at least 9 columns; 10th is optional 'halo'
        if len(parts) >= 9:
            rec = {
                'CME': parts[0],
                't0': parts[1],
                'dt0': parts[2],
                'pa': parts[3],
                'da': parts[4],
                'v': parts[5],
                'dv': parts[6],
                'minv': parts[7],
                'maxv': parts[8],
                'halo': parts[9] if len(parts) > 9 else ''
            }
            records.append(rec)

    df = pd.DataFrame(records)

    # Convert data types
    df['dt0'] = pd.to_numeric(df['dt0'], errors='coerce').astype('Int64')
    df['pa'] = pd.to_numeric(df['pa'], errors='coerce').astype('Int64')
    df['da'] = pd.to_numeric(df['da'], errors='coerce').astype('Int64')
    df['v'] = pd.to_numeric(df['v'], errors='coerce').astype('Int64')
    df['dv'] = pd.to_numeric(df['dv'], errors='coerce').astype('Int64')
    df['minv'] = pd.to_numeric(df['minv'], errors='coerce').astype('Int64')
    df['maxv'] = pd.to_numeric(df['maxv'], errors='coerce').astype('Int64')

    # Convert t0 to datetime
    df['t0'] = pd.to_datetime(df['t0'], format='%Y/%m/%d %H:%M', errors='coerce')

    return df


def save_to_csv(df: pd.DataFrame, year: int, month: int, output_dir: str):
    """
    Save the DataFrame to a CSV file named cme_{year}_{month:02d}.csv in the output directory.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = f"cme_{year}_{month:02d}.csv"
    path = os.path.join(output_dir, filename)
    df.to_csv(path, index=False)
    print(f"Saved CSV to {path}")


def main():
    parser = argparse.ArgumentParser(description='Scrape and convert CACTus CME catalog to CSV')
    parser.add_argument('--year', type=int, required=True, help='Year of the catalog (e.g., 2025)')
    parser.add_argument('--month', type=int, required=True, help='Month of the catalog (1-12)')
    parser.add_argument('--output', type=str, default='cme_data', help='Output directory for CSV files')
    args = parser.parse_args()

    print(f"Fetching CME data for {args.year}-{args.month:02d}...")
    text = fetch_cme_text(args.year, args.month)

    print("Parsing data...")
    df = parse_cme_data_from_text(text)

    print(f"Total events parsed: {len(df)}")
    print(df.head())

    print("Saving to CSV...")
    save_to_csv(df, args.year, args.month, args.output)

    print("Done.")


if __name__ == '__main__':
    import sys
    sys.argv = ['cme_june.py', '--year', '2025', '--month', '05']
    main()

