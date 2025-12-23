import os
import pandas as pd
import re
from datetime import datetime
import warnings

# Suppress pandas datetime parsing warnings
warnings.filterwarnings('ignore')
# CONFIGURATION: Set your CSV directory path here
INPUT_CSV_DIRECTORY = r""

def clean_column_headers(df):
    """Clean column headers by replacing special characters and removing whitespace"""
    df.columns = (df.columns
                  .str.strip()                     # Remove leading/trailing whitespace
                  .str.replace(' ', '_')           # Replace spaces with underscores
                  .str.replace('-', '_')           # Replace hyphens with underscores
                  .str.replace('#', 'Num')         # Replace # with Num
                  .str.replace('$', 'Dol')         # Replace $ with Dol
                  .str.replace('%', 'pct')         # Replace % with pct
                  .str.replace('/', '')            # Remove / characters
                  .str.replace('@', '')            # Remove @ characters
                  .str.replace('*', '')            # Remove * characters
                  .str.replace('^', '')            # Remove ^ characters
                  .str.replace('&', '')            # Remove & characters
                  .str.replace('(', '')            # Remove ( characters
                  .str.replace(')', '')            # Remove ) characters
                  .str.replace('{', '')            # Remove { characters
                  .str.replace('}', '')            # Remove } characters
                  .str.replace('[', '')            # Remove [ characters
                  .str.replace(']', ''))           # Remove ] characters
    return df

def detect_and_clean_dates(value, column_name=''):
    """Detect and convert various date formats using pandas to_datetime"""
    
    # Handle missing or null-like values
    if pd.isna(value) or str(value).strip() == "":
        return value
    
    value_str = str(value).strip()
    
    # Skip percentage/rate columns
    if any(term in column_name.lower() for term in ['pct', 'percent', 'rate']):
        return value
    
    # If it's already a clean decimal number (typical for percentages), don't convert
    try:
        float_val = float(value_str)
        if -1 < float_val < 1 and '.' in value_str:
            return value
    except ValueError:
        pass  # Not a number, continue with date detection
    
    # Check if column name suggests this is a date column (for YYYYMMDD format detection)
    date_keywords = [
        'date', 'time', 'timestamp', 'datetime',
        'created', 'updated', 'modified', 'deleted',
        'start', 'end', 'due', 'expire', 'birth', 'dob'
    ]
    date_suffixes = ['_at', '_on']  # Common database conventions
    
    is_date_column = (
        any(keyword in column_name.lower() for keyword in date_keywords) or
        any(column_name.lower().endswith(suffix) for suffix in date_suffixes)
    )
    
    # Skip conversion if it's just a number and not in a date column
    # This prevents converting things like "20240115" when it's not meant to be a date
    if value_str.isdigit() and len(value_str) == 8 and not is_date_column:
        return value
    
    # Check if the original value contains time information (HH:MM pattern)
    has_time_in_original = bool(re.search(r'\d{1,2}:\d{2}', value_str))
    
    try:
        # Try to parse the date using pandas
        parsed_date = pd.to_datetime(value_str, errors='coerce')
        
        # If parsing failed, return original value
        if pd.isna(parsed_date):
            return value
        
        # If the original had time, return with time
        if has_time_in_original:
            # Include microsecond precision only if needed
            if parsed_date.microsecond > 0:
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip('0').rstrip('.')
            else:
                return parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # Original had no time, return date only
            return parsed_date.strftime("%Y-%m-%d")
    
    except (ValueError, TypeError):
        # If any error occurs, return the original value
        return value

def clean_currency_and_numbers(value):
    """Remove $ signs and commas from numerical values only"""
    if pd.isna(value):
        return value
    
    value_str = str(value).strip()
    
    # Only process if it looks like a number with $ or commas
    # Check if the value (after removing $ and commas) is a valid number
    cleaned = value_str.replace('$', '').replace(',', '').strip()
    
    try:
        # Try to convert to float - if successful, it was a number
        float_val = float(cleaned)
        # Return the cleaned numeric string
        return cleaned
    except ValueError:
        # If conversion fails, return original value unchanged
        # This means it's text that happens to contain $ or commas
        return value

def clean_percentage(value):
    """Convert percentage values to decimals by removing % and dividing by 100"""
    if pd.isna(value):
        return value
    
    value_str = str(value).strip()
    
    # Check if value contains %
    if '%' in value_str:
        # Remove % and any other non-numeric characters except decimal point and negative sign
        cleaned = value_str.replace('%', '').replace(',', '').strip()
        try:
            # Convert to float and divide by 100
            decimal_val = float(cleaned) / 100
            return decimal_val
        except ValueError:
            # If conversion fails, just remove the % sign
            return cleaned
    
    return value

def write_log(log_file, message):
    """Write a message to the log file"""
    with open(log_file, 'a') as f:
        f.write(message + '\n')

def audit_dataframe(df_original, df_cleaned, csv_file, log_path):
    """Audit the cleaned dataframe against the original to ensure data integrity"""
    write_log(log_path, "")
    write_log(log_path, "=== DATA AUDIT ===")
    
    audit_passed = True
    
    # Check row counts
    if len(df_original) != len(df_cleaned):
        write_log(log_path, f"WARNING: Row count mismatch! Original: {len(df_original)}, Cleaned: {len(df_cleaned)}")
        audit_passed = False
    
    # Check column counts
    if len(df_original.columns) != len(df_cleaned.columns):
        write_log(log_path, f"WARNING: Column count mismatch! Original: {len(df_original.columns)}, Cleaned: {len(df_cleaned.columns)}")
        audit_passed = False
    
    # Audit each column
    for orig_col, clean_col in zip(df_original.columns, df_cleaned.columns):
        try:
            orig_series = df_original[orig_col]
            clean_series = df_cleaned[clean_col]
            
            # Check null counts
            orig_nulls = orig_series.isna().sum()
            clean_nulls = clean_series.isna().sum()
            
            if orig_nulls != clean_nulls:
                write_log(log_path, f"WARNING: Column '{clean_col}': Null count changed from {orig_nulls} to {clean_nulls}")
                audit_passed = False
            
            # Try to detect column type and perform appropriate checks
            # Check if numeric
            try:
                orig_numeric = pd.to_numeric(orig_series, errors='coerce')
                clean_numeric = pd.to_numeric(clean_series, errors='coerce')
                
                # If most values are numeric, treat as numeric column
                if orig_numeric.notna().sum() / len(orig_numeric) > 0.8:
                    # Numeric column - check mean, min, max, sum
                    orig_mean = orig_numeric.mean()
                    clean_mean = clean_numeric.mean()
                    orig_min = orig_numeric.min()
                    clean_min = clean_numeric.min()
                    orig_max = orig_numeric.max()
                    clean_max = clean_numeric.max()
                    orig_sum = orig_numeric.sum()
                    clean_sum = clean_numeric.sum()
                    
                    # Allow small tolerance for floating point differences
                    tolerance = 0.01
                    
                    if abs(orig_mean - clean_mean) > tolerance:
                        write_log(log_path, f"WARNING: Column '{clean_col}': Mean changed from {orig_mean:.4f} to {clean_mean:.4f}")
                        audit_passed = False
                    if abs(orig_min - clean_min) > tolerance:
                        write_log(log_path, f"WARNING: Column '{clean_col}': Min changed from {orig_min:.4f} to {clean_min:.4f}")
                        audit_passed = False
                    if abs(orig_max - clean_max) > tolerance:
                        write_log(log_path, f"WARNING: Column '{clean_col}': Max changed from {orig_max:.4f} to {clean_max:.4f}")
                        audit_passed = False
                    if abs(orig_sum - clean_sum) > tolerance:
                        write_log(log_path, f"WARNING: Column '{clean_col}': Sum changed from {orig_sum:.4f} to {clean_sum:.4f}")
                        audit_passed = False
                    
                    continue  # Skip other checks for numeric columns
            except:
                pass
            
            # Check if date column
            try:
                orig_dates = pd.to_datetime(orig_series, errors='coerce', infer_datetime_format=True)
                clean_dates = pd.to_datetime(clean_series, errors='coerce', infer_datetime_format=True)
                
                # If most values are dates, treat as date column
                if orig_dates.notna().sum() / len(orig_dates) > 0.8:
                    orig_date_count = orig_dates.notna().sum()
                    clean_date_count = clean_dates.notna().sum()
                    
                    if orig_date_count != clean_date_count:
                        write_log(log_path, f"WARNING: Column '{clean_col}': Valid date count changed from {orig_date_count} to {clean_date_count}")
                        audit_passed = False
                    
                    if orig_date_count > 0 and clean_date_count > 0:
                        orig_min_date = orig_dates.min()
                        clean_min_date = clean_dates.min()
                        orig_max_date = orig_dates.max()
                        clean_max_date = clean_dates.max()
                        
                        # For dates, just check the date part (ignore time for comparison)
                        if orig_min_date.date() != clean_min_date.date():
                            write_log(log_path, f"WARNING: Column '{clean_col}': Min date changed from {orig_min_date.date()} to {clean_min_date.date()}")
                            audit_passed = False
                        if orig_max_date.date() != clean_max_date.date():
                            write_log(log_path, f"WARNING: Column '{clean_col}': Max date changed from {orig_max_date.date()} to {clean_max_date.date()}")
                            audit_passed = False
                    
                    continue  # Skip other checks for date columns
            except:
                pass
            
            # String column - check unique counts and most common value
            orig_unique = orig_series.nunique()
            clean_unique = clean_series.nunique()
            orig_count = orig_series.notna().sum()
            clean_count = clean_series.notna().sum()
            
            if orig_count != clean_count:
                write_log(log_path, f"WARNING: Column '{clean_col}': Non-null count changed from {orig_count} to {clean_count}")
                audit_passed = False
            
            if orig_unique != clean_unique:
                write_log(log_path, f"WARNING: Column '{clean_col}': Unique value count changed from {orig_unique} to {clean_unique}")
                audit_passed = False
            
        except Exception as e:
            write_log(log_path, f"WARNING: Column '{clean_col}': Error during audit - {str(e)}")
            audit_passed = False
    
    if audit_passed:
        write_log(log_path, "[PASS] Audit PASSED: Data integrity maintained")
    else:
        write_log(log_path, "[FAIL] Audit FAILED: Data integrity issues detected (see warnings above)")
    
    write_log(log_path, "")
    
    return audit_passed

def clean_csv_files(input_directory, output_directory, log_directory):
    """Clean all CSV files in the input directory and save to output directory with logging"""
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print(f"Created output directory: {output_directory}")
    
    # Create log directory if it doesn't exist
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
        print(f"Created log directory: {log_directory}")
    
    # Create log file with timestamp
    log_filename = f"cleaning_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_path = os.path.join(log_directory, log_filename)
    
    # Initialize log file
    write_log(log_path, "="*80)
    write_log(log_path, f"CSV Cleaning Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    write_log(log_path, "="*80)
    write_log(log_path, f"Input Directory: {input_directory}")
    write_log(log_path, f"Output Directory: {output_directory}")
    write_log(log_path, "")
    
    # Get all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]
    
    if not csv_files:
        message = f"No CSV files found in {input_directory}"
        print(message)
        write_log(log_path, message)
        return
    
    print(f"Found {len(csv_files)} CSV file(s) to clean\n")
    write_log(log_path, f"Found {len(csv_files)} CSV file(s) to clean")
    write_log(log_path, "")
    
    for csv_file in csv_files:
        print(f"Processing: {csv_file}")
        write_log(log_path, "-"*80)
        write_log(log_path, f"File: {csv_file}")
        
        input_path = os.path.join(input_directory, csv_file)
        output_path = os.path.join(output_directory, csv_file)
        
        try:
            # Read CSV file
            df_original = pd.read_csv(input_path)
            df = df_original.copy()  # Create a copy for cleaning
            original_rows = len(df)
            original_cols = len(df.columns)
            
            write_log(log_path, f"Original dimensions: {original_rows} rows × {original_cols} columns")
            
            # Check for auto-generated column headers (Unnamed columns)
            unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed:')]
            if unnamed_cols:
                write_log(log_path, "ALERT: Auto-generated column headers detected (rows have more values than headers):")
                for col in unnamed_cols:
                    write_log(log_path, f"  - '{col}' was automatically created")
            
            # Clean column headers
            df = clean_column_headers(df)
            
            # Check for long column headers (>32 characters)
            long_headers = [col for col in df.columns if len(col) > 32]
            if long_headers:
                write_log(log_path, "WARNING: Column headers exceeding 32 characters:")
                for header in long_headers:
                    write_log(log_path, f"  - '{header}' ({len(header)} characters)")
            
            # Strip leading/trailing whitespace from all values
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            
            # Apply cleaning to each cell in the dataframe
            for col in df.columns:
                df[col] = df[col].apply(lambda x: clean_currency_and_numbers(x))
                df[col] = df[col].apply(lambda x: clean_percentage(x))
                
                # Check if column is purely numeric (skip date conversion for numeric columns)
                # This prevents converting revenue, prices, IDs, etc.
                try:
                    # Try to convert the entire column to numeric
                    # If successful, it's a numeric column and should skip date conversion
                    pd.to_numeric(df[col], errors='raise')
                    is_numeric_column = True
                except (ValueError, TypeError):
                    is_numeric_column = False
                
                # Only apply date cleaning if it's NOT a purely numeric column
                if not is_numeric_column:
                    df[col] = df[col].apply(lambda x: detect_and_clean_dates(x, col))
            
            cleaned_rows = len(df)
            cleaned_cols = len(df.columns)
            
            # Perform data audit
            audit_passed = audit_dataframe(df_original, df, csv_file, log_path)
            
            # Save cleaned CSV
            df.to_csv(output_path, index=False)
            
            write_log(log_path, f"Cleaned dimensions: {cleaned_rows} rows × {cleaned_cols} columns")
            write_log(log_path, f"Status: SUCCESS")
            write_log(log_path, f"Output: {output_path}")
            print(f"  [OK] Cleaned and saved to: {output_path}")
            
        except Exception as e:
            error_message = f"ERROR: {str(e)}"
            write_log(log_path, error_message)
            print(f"  [ERROR] Error processing {csv_file}: {str(e)}")
        
        write_log(log_path, "")
    
    write_log(log_path, "="*80)
    write_log(log_path, f"Processing completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    write_log(log_path, "="*80)
    
    print(f"\nAll files processed!")
    print(f"Cleaned files saved to: {output_directory}")
    print(f"Log file saved to: {log_path}")

if __name__ == "__main__":
    # Use the constant defined at the top of the script
    input_dir = INPUT_CSV_DIRECTORY
    
    # Check if directory exists
    if not os.path.exists(input_dir):
        print(f"Error: Directory '{input_dir}' does not exist!")
        print("Please update the INPUT_CSV_DIRECTORY constant at the top of the script.")
        exit(1)
    
    # Set output directory (creates a 'cleaned_csvs' folder inside the input directory)
    output_dir = os.path.join(input_dir, 'cleaned_csvs')
    
    # Set log directory (creates a 'logs' folder inside the input directory)
    log_dir = os.path.join(input_dir, 'logs')
    
    # Clean the CSV files
    clean_csv_files(input_dir, output_dir, log_dir)
