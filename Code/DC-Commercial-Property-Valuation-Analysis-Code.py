# Title: DC Commercial Property Valuation Analysis
# Author: Alexander Zakrzeski
# Date: March 18, 2026

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
          pl.col("num_units").fill_null(0),
          pl.col("sale_date").str.to_datetime("%Y/%m/%d %H:%M:%S%#z").dt.year() 
            .alias("sale_year"),
          pl.col("qualified").str.strip_chars()
          )
      .filter((pl.col("bldg_num") == 1) & 
              (pl.col("num_units") <= 1_000) & 
              (pl.col("qualified") == "Q") & 
              (pl.col("ayb").is_between(1900, 2025)) & 
              (((pl.col("sale_year") >= pl.col("yr_rmdl")) & 
                (pl.col("yr_rmdl") >= pl.col("ayb"))) | 
               pl.col("yr_rmdl").is_null()))
      .with_columns(
          pl.col("ssl").str.replace_all(r"\s+", " "), 
          (pl.col("sale_year") - pl.col("ayb")).alias("age")
          )
      .drop("objectid", "bldg_num", "sect_num", "struct_cl", "struct_cl_d", 
            "grade", "extwall", "extwall_d", "qualified", "ayb", "yr_rmdl", 
            "eyb", "gis_last_mod_dttm")
    )