# Title: DC Commercial Property Valuation Analysis
# Author: Alexander Zakrzeski
# Date: March 19, 2026

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
              pl.col("num_units").is_between(0, 1_000) &
              (pl.col("qualified") == "Q") &
              pl.col("sale_year").is_between(2019, 2025) &
              (pl.col("sale_year") >= pl.col("ayb")) &
              pl.col("ayb").is_between(1900, 2025) &
              (((pl.col("sale_year") >= pl.col("yr_rmdl")) & 
                (pl.col("yr_rmdl") >= pl.col("ayb"))) | 
               pl.col("yr_rmdl").is_null()) &
              pl.col("use_code").is_in([31, 32, 33, 37, 39, 41, 42, 43, 44, 45, 
                                        46, 47, 48, 49, 51, 52, 53, 56, 57, 58,
                                        59, 61, 62, 63, 65, 66, 67, 68, 69, 71, 
                                        73, 74, 75, 76, 78, 79, 165, 265, 365, 
                                        465]))
      .with_columns(
          pl.col("ssl").str.replace_all(r"\s+", " "),
          (pl.col("sale_year") - pl.col("ayb")).alias("age"),
          (pl.col("yr_rmdl").is_not_null()).cast(pl.Int8).alias("rmdl")
          )
      .drop("objectid", "bldg_num", "sect_num", "struct_cl", "struct_cl_d", 
            "grade", "grade_d", "extwall", "extwall_d", "wall_hgt", "qualified", 
            "ayb", "yr_rmdl", "eyb", "gis_last_mod_dttm")
    )

# Update the commercial DataFrame retaining the relevant records and columns 
commercial = (
    commercial.filter(pl.col("sale_num").is_between(1, 6) &
                      (pl.col("living_gba") > 0))
    )         