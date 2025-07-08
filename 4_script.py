import pandas as pd
import os
from datetime import datetime

def read_cdf_data(cdf_folder):
    """Read and concatenate all CDF CSV files"""
    print(f"Reading CDF data from {cdf_folder}...")
    
    # Get all CSV files in the folder
    cdf_files = [os.path.join(cdf_folder, f) for f in os.listdir(cdf_folder) if f.endswith('.csv')]
    
    if not cdf_files:
        print(f"No CSV files found in {cdf_folder}")
        return None
    
    print(f"Found {len(cdf_files)} CSV files")
    
    # Read and concatenate all files
    df_list = []
    for file in cdf_files:
        try:
            df = pd.read_csv(file)
            df_list.append(df)
            print(f"  - Loaded {file}: {len(df)} rows")
        except Exception as e:
            print(f"  - Error loading {file}: {e}")
    
    if df_list:
        df_cdf = pd.concat(df_list, ignore_index=True)
        print(f"Total CDF data: {len(df_cdf)} rows")
        return df_cdf
    else:
        return None

def read_cme_data(cme_folder):
    """Read and concatenate all CME CSV files"""
    print(f"\nReading CME data from {cme_folder}...")
    
    # Get all CSV files in the folder
    cme_files = [os.path.join(cme_folder, f) for f in os.listdir(cme_folder) if f.endswith('.csv')]
    
    if not cme_files:
        print(f"No CSV files found in {cme_folder}")
        return None
    
    print(f"Found {len(cme_files)} CSV files")
    
    # Read and concatenate all files
    df_list = []
    for file in cme_files:
        try:
            df = pd.read_csv(file)
            df_list.append(df)
            print(f"  - Loaded {file}: {len(df)} rows")
        except Exception as e:
            print(f"  - Error loading {file}: {e}")
    
    if df_list:
        df_cme = pd.concat(df_list, ignore_index=True)
        print(f"Total CME data: {len(df_cme)} rows")
        return df_cme
    else:
        return None

def extract_cme_matching_data(df_cdf, df_cme, output_filename='cme_matched_data.csv'):
    """Extract CDF data that matches CME event datetimes by hour and minute (ignoring seconds and smaller units).
    For each CME event, aggregate matching CDF rows: mean for numerics, mode for objects."""
    print(f"\nExtracting matching data...")

    # Convert to datetime
    df_cdf['datetime'] = pd.to_datetime(df_cdf['epoch_for_cdf_mod'])
    df_cme['datetime'] = pd.to_datetime(df_cme['t0'])

    # Truncate to year, month, day, hour, minute (ignore seconds and smaller units)
    df_cdf['dt_trunc'] = df_cdf['datetime'].dt.floor('min')
    df_cme['dt_trunc'] = df_cme['datetime'].dt.floor('min')

    # Prepare output list
    output_rows = []
    print(f"Processing {len(df_cme)} CME events...")

    # Identify numerical and object columns in CDF (excluding datetime columns)
    num_cols = df_cdf.select_dtypes(include='number').columns.tolist()
    obj_cols = df_cdf.select_dtypes(include='object').columns.tolist()
    # Remove helper columns if present
    for col in ['epoch_for_cdf_mod', 'datetime', 'dt_trunc']:
        if col in num_cols: num_cols.remove(col)
        if col in obj_cols: obj_cols.remove(col)

    for idx, cme_row in df_cme.iterrows():
        cme_time = cme_row['dt_trunc']
        # Find all CDF rows matching this CME event (by year, month, day, hour, minute)
        matches = df_cdf[df_cdf['dt_trunc'] == cme_time]
        if not matches.empty:
            # Aggregate: mean for numerics, mode for objects
            agg_row = {}
            for col in num_cols:
                agg_row[col] = matches[col].mean()
            for col in obj_cols:
                agg_row[col] = matches[col].mode().iloc[0] if not matches[col].mode().empty else None
            # Optionally, add CME info to output (e.g., t0)
            agg_row['cme_t0'] = cme_row['t0']
            output_rows.append(agg_row)
        else:
            # Optionally, add row with NaNs if no match found
            agg_row = {col: None for col in num_cols + obj_cols}
            agg_row['cme_t0'] = cme_row['t0']
            output_rows.append(agg_row)

    # Create DataFrame and save
    result_df = pd.DataFrame(output_rows)
    result_df.to_csv(output_filename, index=False)
    print(f"\nAggregated matched data saved to: {output_filename}")
    print(f"Total CME events processed: {len(df_cme)}")
    print(f"Rows in output: {len(result_df)}")

    return result_df

def main():
    """Main function to run the data extraction"""
    # Define folder paths
    cdf_folder = 'cdf_data'
    cme_folder = 'cme_data'
    
    # Check if folders exist
    if not os.path.exists(cdf_folder):
        print(f"Error: {cdf_folder} folder not found!")
        return
    
    if not os.path.exists(cme_folder):
        print(f"Error: {cme_folder} folder not found!")
        return
    
    # Read data
    df_cdf = read_cdf_data(cdf_folder)
    df_cme = read_cme_data(cme_folder)
    
    if df_cdf is None or df_cme is None:
        print("Error: Could not load data from one or both folders!")
        return
    
    # Extract matching data
    matched_data = extract_cme_matching_data(df_cdf, df_cme)
    
    print(f"\n{'='*50}")
    print("DATA EXTRACTION COMPLETE!")
    print(f"{'='*50}")
    
    return matched_data

if __name__ == "__main__":
    # Run the script
    matched_data = main()