# CSV Cleaner

Automatically cleans and standardizes CSV files for database import, with data integrity auditing.

## Overview

This script processes CSV files in a designated folder and applies the following transformations:

- **Column headers**: Replaces special characters (`#`, `$`, `%`, spaces, etc.) with database-safe alternatives
- **Date standardization**: Converts various date formats to `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`
- **Currency cleaning**: Removes `$` signs and commas from numeric values
- **Percentage conversion**: Converts `25%` to `0.25`
- **Whitespace trimming**: Strips leading/trailing whitespace from all values

After cleaning, an audit compares the original and cleaned data to verify integrity (row counts, null counts, numeric sums, date ranges, etc.).

### Folder Structure
```
your_csv_folder/
├── sales_data.csv           (input)
├── customer_list.csv        (input)
├── cleaned_csvs/            (created automatically)
│   ├── sales_data.csv
│   └── customer_list.csv
└── logs/                    (created automatically)
    └── cleaning_log_20240115_103000.txt
```

## Installation

### Prerequisites

- Python 3.7+

### Install Dependencies
```bash
pip install pandas
```

## Configuration

Edit the constant at the top of the script:
```python
INPUT_CSV_DIRECTORY = r"c:\path\to\your\csv\folder"
```

## Usage

1. Place CSV files in your configured input directory
2. Run the script:
```bash
python csv_cleaner.py
```

Cleaned files appear in the `cleaned_csvs` subfolder, with a detailed log in the `logs` subfolder.

### Column Header Transformations

| Original Character | Replacement |
|--------------------|-------------|
| Space | `_` |
| `-` | `_` |
| `#` | `Num` |
| `$` | `Dol` |
| `%` | `pct` |
| `/ @ * ^ & ( ) { } [ ]` | Removed |

### Date Format Handling

The script auto-detects and standardizes dates:

| Input Format | Output |
|--------------|--------|
| `01/15/2024` | `2024-01-15` |
| `January 15, 2024` | `2024-01-15` |
| `15-Jan-2024` | `2024-01-15` |
| `2024/01/15 14:30:00` | `2024-01-15 14:30:00` |

Date conversion is skipped for columns that are purely numeric to prevent false positives (e.g., revenue figures or IDs).

### Logs

Each run generates a timestamped log file containing:

- Files processed and their dimensions
- Warnings for long column headers (>32 characters)
- Alerts for auto-generated column names
- Audit results comparing original vs. cleaned data
