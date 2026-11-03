# Dataset Instructions

## Dataset Source

This project uses the Brazilian E-Commerce Public Dataset by Olist.

**Download link:**  
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Installation Instructions

1. Download the dataset from Kaggle.
2. Extract the downloaded ZIP file.
3. Copy the CSV files into the `data/raw/` directory.

## Required CSV Files

The initial Python ingestion module uses these three files:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_products_dataset.csv`

Additional dataset files may be used for extended analytics and data modeling.

## Directory Structure

```text
data/
├── README.md
├── raw/
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── olist_products_dataset.csv
└── processed/
    └── order_events.jsonl
```

## Data Processing

To generate order-item events from the raw CSV files, run this command from the project root:

```powershell
python member1_ingestion/data_loader.py
```

The generated JSON Lines file will be saved in `data/processed/`.

## GitHub Note

Raw dataset files and generated processed data are excluded from GitHub to avoid uploading large data files.

Refer to the original Kaggle dataset page for attribution and licence terms.