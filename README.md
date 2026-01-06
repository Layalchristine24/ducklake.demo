
<!-- README.md is generated from README.Rmd. Please edit that file -->

# DuckLake Demo

<!-- badges: start -->

<!-- badges: end -->

A 5-minute demonstration of **DuckLake** - a lakehouse format directly
in DuckDB with metadata in SQL, time travel, ACID transactions, schema
evolution, and MERGE INTO support.

## Setup

``` r
library(duckplyr)

# Setup paths and clean slate
path <- "metadata_files"
unlink(path, recursive = TRUE)
dir.create(path, showWarnings = FALSE)

# Get a DuckDB connection
con <- duckplyr:::get_default_duckdb_connection()

# Load DuckLake extension
# DBI::dbExecute(con, "INSTALL ducklake")
DBI::dbExecute(con, "LOAD ducklake")
#> [1] 0
```

## 1. Create a DuckLake Database

Metadata is stored in SQLite, data in Parquet files.

``` r
DBI::dbExecute(
  con,
  sprintf("ATTACH 'ducklake:%s/metadata.ducklake' AS lake", path)
)
#> [1] 0

DBI::dbGetQuery(con, "SHOW DATABASES")
#>              database_name
#> 1 __ducklake_metadata_lake
#> 2     duckplyr9df71937c7e7
#> 3                     lake
```

## 2. Create Table and Insert Data

``` r
DBI::dbExecute(
  con,
  "CREATE TABLE lake.customers (
    id INTEGER,
    name VARCHAR,
    email VARCHAR
  )"
)
#> [1] 0

DBI::dbExecute(
  con,
  "INSERT INTO lake.customers (id, name, email) VALUES
   (1, 'Alice', 'alice@example.com'),
   (2, 'Bob', 'bob@example.com'),
   (3, 'Charlie', 'charlie@example.com')"
)
#> [1] 3

# Query with duckplyr
tbl(con, I("lake.customers")) |> collect()
#> # A tibble: 3 × 3
#>      id name    email              
#>   <int> <chr>   <chr>              
#> 1     1 Alice   alice@example.com  
#> 2     2 Bob     bob@example.com    
#> 3     3 Charlie charlie@example.com
```

## 3. Time Travel

``` r
# Get current snapshot version
snapshots <- DBI::dbGetQuery(con, "SELECT * FROM ducklake_snapshots('lake')")
snapshot_before <- max(snapshots$snapshot_id)

# Make changes
DBI::dbExecute(
  con,
  "UPDATE lake.customers SET email = 'alice.new@example.com' WHERE id = 1"
)
#> [1] 1
DBI::dbExecute(
  con,
  "INSERT INTO lake.customers VALUES (4, 'Diana', 'diana@example.com')"
)
#> [1] 1

# Current data (after changes)
tbl(con, I("lake.customers")) |> collect()
#> # A tibble: 4 × 3
#>      id name    email                
#>   <int> <chr>   <chr>                
#> 1     2 Bob     bob@example.com      
#> 2     3 Charlie charlie@example.com  
#> 3     1 Alice   alice.new@example.com
#> 4     4 Diana   diana@example.com

# Time travel to previous version
DBI::dbGetQuery(
  con,
  sprintf("SELECT * FROM lake.customers AT (VERSION => %s)", snapshot_before)
)
#>   id    name               email
#> 1  1   Alice   alice@example.com
#> 2  2     Bob     bob@example.com
#> 3  3 Charlie charlie@example.com
```

## 4. Schema Evolution

``` r
DBI::dbExecute(
  con,
  "ALTER TABLE lake.customers ADD COLUMN status VARCHAR DEFAULT 'active'"
)
#> [1] 0

tbl(con, I("lake.customers")) |> collect()
#> # A tibble: 4 × 4
#>      id name    email                 status
#>   <int> <chr>   <chr>                 <chr> 
#> 1     2 Bob     bob@example.com       active
#> 2     3 Charlie charlie@example.com   active
#> 3     1 Alice   alice.new@example.com active
#> 4     4 Diana   diana@example.com     active
```

## 5. MERGE INTO (Upsert)

``` r
# Create source table with updates
DBI::dbExecute(
  con,
  "CREATE TEMP TABLE updates AS
   SELECT * FROM (VALUES
     (1, 'Alice', 'alice.updated@example.com', 'vip'),
     (5, 'Eve', 'eve@example.com', 'new')
   ) AS t(id, name, email, status)"
)
#> [1] 2

DBI::dbExecute(
  con,
  "MERGE INTO lake.customers AS target
   USING updates AS source
   ON target.id = source.id
   WHEN MATCHED THEN UPDATE SET
     email = source.email,
     status = source.status
   WHEN NOT MATCHED THEN INSERT (id, name, email, status)
     VALUES (source.id, source.name, source.email, source.status)"
)
#> [1] 2

tbl(con, I("lake.customers")) |> arrange(id) |> collect()
#> # A tibble: 5 × 4
#>      id name    email                     status
#>   <int> <chr>   <chr>                     <chr> 
#> 1     1 Alice   alice.updated@example.com vip   
#> 2     2 Bob     bob@example.com           active
#> 3     3 Charlie charlie@example.com       active
#> 4     4 Diana   diana@example.com         active
#> 5     5 Eve     eve@example.com           new
```

## 6. Metadata Inspection

``` r
# Version history
DBI::dbGetQuery(con, "SELECT * FROM ducklake_snapshots('lake')")
#>   snapshot_id       snapshot_time schema_version
#> 1           0 2026-01-06 09:44:41              0
#> 2           1 2026-01-06 09:44:41              1
#> 3           2 2026-01-06 09:44:42              1
#> 4           3 2026-01-06 09:44:42              1
#> 5           4 2026-01-06 09:44:42              1
#> 6           5 2026-01-06 09:44:42              2
#> 7           6 2026-01-06 09:44:42              2
#>                                           changes author commit_message
#> 1                           schemas_created, main   <NA>           <NA>
#> 2                  tables_created, main.customers   <NA>           <NA>
#> 3                         tables_inserted_into, 1   <NA>           <NA>
#> 4 tables_inserted_into, tables_deleted_from, 1, 1   <NA>           <NA>
#> 5                         tables_inserted_into, 1   <NA>           <NA>
#> 6                               tables_altered, 1   <NA>           <NA>
#> 7 tables_inserted_into, tables_deleted_from, 1, 1   <NA>           <NA>
#>   commit_extra_info
#> 1              <NA>
#> 2              <NA>
#> 3              <NA>
#> 4              <NA>
#> 5              <NA>
#> 6              <NA>
#> 7              <NA>

# Table info
DBI::dbGetQuery(con, "SELECT * FROM ducklake_table_info('lake')")
#>   table_name schema_id table_id                           table_uuid file_count
#> 1  customers         0        1 019b92b1-c2f8-7ff0-baa5-55fc423067f3          4
#>   file_size_bytes delete_file_count delete_file_size_bytes
#> 1            2373                 1                    844
```

## Key Takeaways

1.  **Metadata in SQL** - Simple, queryable metadata storage
2.  **Parquet data files** - Efficient, portable data format
3.  **Time Travel** - Auditing and recovery capabilities
4.  **ACID transactions** - Data integrity guarantees
5.  **Schema Evolution** - Add columns without rewriting data
6.  **MERGE INTO** - Easy upsert operations

Perfect for data lakes that need versioning + transactions without the
complexity of Iceberg/Delta Lake!

## Cleanup

``` r
DBI::dbExecute(con, "DETACH lake")
#> [1] 0
DBI::dbDisconnect(con, shutdown = TRUE)
unlink(path, recursive = TRUE)
```
