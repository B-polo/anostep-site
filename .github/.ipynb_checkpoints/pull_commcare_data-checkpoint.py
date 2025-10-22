import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime

# Configuration
COMMCARE_URL = "https://www.commcarehq.org/a/atsb-project-1/api/odata/forms/v1/b81194d6348b69e2ce1880689777a556/feed"
OUTPUT_DIR = "data"

# # Get credentials
# email = os.environ.get('COMMCARE_EMAIL')
# password = os.environ.get('COMMCARE_PASSWORD')

# credentials
email = "otienobrn09@gmail.com"
password = "Tracy@2013"

if not email or not password:
    print("❌ Error: Set COMMCARE_EMAIL and COMMCARE_PASSWORD environment variables")
    print("Example: export COMMCARE_EMAIL='your_email@example.com'")
    exit(1)


def clean_column_names(df):
    """Remove 'form ' prefix and clean special characters from column names"""
    new_columns = {}
    
    for col in df.columns:
        # Remove 'form ' prefix
        new_col = col[5:] if col.lower().startswith('form ') else col
        
        # Replace special characters
        new_col = new_col.replace(' | ', '_').replace(': ', '_').replace(' ', '_')
        new_col = new_col.replace('__', '_').strip('_')
        
        new_columns[col] = new_col
    
    return df.rename(columns=new_columns)


def download_data():
    """Download data from CommCare OData API"""
    print(f"\n🕐 Starting download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📡 Connecting to CommCare...")
    
    try:
        # Make API request
        response = requests.get(
            COMMCARE_URL,
            auth=HTTPBasicAuth(email, password),
            timeout=60
        )
        response.raise_for_status()
        
        # Get data
        data = response.json()
        
        if not data.get('value'):
            print("❌ No data found")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(data['value'])
        print(f"✅ Retrieved {len(df):,} records with {len(df.columns)} columns")
        
        return df
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error {e.response.status_code}: {e.response.reason}")
        if e.response.status_code == 401:
            print("   Check your email and password")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def save_data(df):
    """Save raw and cleaned data to CSV files"""
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Save raw data
    raw_file = os.path.join(OUTPUT_DIR, "commcare_raw_data.csv")
    df.to_csv(raw_file, index=False)
    print(f"💾 Raw data saved: {raw_file}")
    
    # Clean column names
    df_cleaned = clean_column_names(df)
    
    # Filter out rows where anoph_present = '---' (if column exists)
    if 'anoph_present' in df_cleaned.columns:
        initial_rows = len(df_cleaned)
        df_cleaned = df_cleaned[df_cleaned['anoph_present'] != '---']
        removed = initial_rows - len(df_cleaned)
        if removed > 0:
            print(f"🗑️  Removed {removed} rows where anoph_present = '---'")
    
    # Save cleaned data
    cleaned_file = os.path.join(OUTPUT_DIR, "commcare_cleaned_data.csv")
    df_cleaned.to_csv(cleaned_file, index=False)
    print(f"💾 Cleaned data saved: {cleaned_file}")
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"   Raw: {len(df):,} records")
    print(f"   Cleaned: {len(df_cleaned):,} records")
    print(f"   Columns: {len(df_cleaned.columns)}")


def main():
    """Main function"""
    # Download data
    df = download_data()
    
    if df is not None:
        # Save data
        save_data(df)
        print(f"\n✅ Download completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎉 Files saved in '{OUTPUT_DIR}' directory\n")
    else:
        print(f"\n❌ Download failed\n")


if __name__ == "__main__":
    main()