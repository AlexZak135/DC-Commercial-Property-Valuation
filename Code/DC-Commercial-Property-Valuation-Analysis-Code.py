# Title: DC Commercial Property Valuation Analysis
# Author: Alexander Zakrzeski
# Date: March 16, 2026

# Part 1: Setup and Configuration

# Load to import, clean, and wrangle data
import polars as pl

# Part 2: Function Definitions

# Part 3: Data Preprocessing

# Create the commercial DataFrame containing the appropriate records and columns
commercial = (
    pl.read_parquet("DC-CAMA-Commercial-Data.parquet")
      .rename(str.lower)
      .rename({"saledate": "sale_date", 
               "usecode": "use_code",
               "landarea": "land_area"}) 
      .with_columns(
          pl.col("sale_date").str.to_datetime("%Y/%m/%d %H:%M:%S%#z").dt.year() 
            .alias("sale_year"),
          pl.col("qualified").str.strip_chars()
          )
    )     