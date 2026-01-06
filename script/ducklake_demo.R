# ==============================================================================
# DuckLake Demo - 5 Minute Presentation
# ==============================================================================
# Shows: Metadata in SQL, Time Travel, ACID, Schema Evolution, MERGE INTO

library(duckplyr)
library(cli)

# Setup paths and clean slate
path <- "metadata_files"
unlink(path, recursive = TRUE)
dir.create(path, showWarnings = FALSE)

# Get a DuckDB connection for SQL operations
con <- duckplyr:::get_default_duckdb_connection()

# Install and load DuckLake extension
# DBI::dbExecute(con, "INSTALL ducklake")
DBI::dbExecute(con, "LOAD ducklake")

# Verify installation
DBI::dbGetQuery(
  con,
  "SELECT * FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
)

cli_h1("DuckLake Demo - Lakehouse directly in DuckDB")

# ==============================================================================
# Step 1: Create a DuckLake Database
# ==============================================================================

cli_h2("Creating DuckLake database (metadata in SQLite, data in Parquet)")
DBI::dbExecute(
  con,
  sprintf("ATTACH 'ducklake:%s/metadata.ducklake' AS test_lake", path)
) # Schema name is 'test_lake'
cli_alert_success("DuckLake attached - metadata stored in 'metadata.ducklake'")
# system("file metadata_files/metadata.ducklake")
# system("file metadata_files/metadata.ducklake.wal")

# Verify attachment
DBI::dbGetQuery(con, "SHOW DATABASES")
file.exists(file.path(path, "metadata.ducklake"))

# ==============================================================================
# Step 2: Create a Table and Insert Data
# ==============================================================================

cli_h2("Creating table and inserting data")
DBI::dbExecute(
  con,
  "
    CREATE TABLE test_lake.customers (
        id INTEGER,
        name VARCHAR,
        email VARCHAR
    )
"
)

DBI::dbExecute(
  con,
  "
    INSERT INTO test_lake.customers (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com')
"
)
cli_alert_success("Inserted 3 customers")

# Query with duckplyr
cli_alert_info("Current data:")
customers <- tbl(con, I("test_lake.customers")) |> collect()
print(customers)
# also possible via DBI::dbGetQuery(con, "SELECT * FROM test_lake.customers")
# read parquet files directly:
arrow::read_parquet(file.path(
  "metadata_files/metadata.ducklake.files/main/customers",
  list.files("metadata_files/metadata.ducklake.files/main/customers")
))
# ==============================================================================
# Step 3: Time Travel
# ==============================================================================

cli_h2("Time Travel Demo")

# Get snapshot AFTER initial insert (this is the version with our 3 customers)
snapshots <- DBI::dbGetQuery(
  con,
  "SELECT * FROM ducklake_snapshots('test_lake')"
)
cli_alert_info("Current snapshots (after initial insert):")
print(snapshots)

# Save the latest snapshot version (with 3 customers)
snapshot_before_changes <- max(snapshots$snapshot_id)
cli_alert_info(
  "Saving snapshot version {snapshot_before_changes} as our 'before' state"
)

# Make changes
DBI::dbExecute(
  con,
  "UPDATE test_lake.customers SET email = 'alice.new@example.com' WHERE id = 1"
)
DBI::dbExecute(
  con,
  "INSERT INTO test_lake.customers (id, name, email) VALUES (4, 'Diana', 'diana@example.com')"
)
cli_alert_success("Updated Alice's email and added Diana")

cli_alert_info("Data AFTER changes:")
customers_after <- tbl(con, I("test_lake.customers")) |> collect()
print(customers_after)

# Time travel back to see original data
cli_alert_info(
  "Time travel to version {snapshot_before_changes} (BEFORE changes):"
)
old_data <- DBI::dbGetQuery(
  con,
  sprintf(
    "SELECT * FROM test_lake.customers AT (VERSION => %s)",
    snapshot_before_changes
  )
)
print(old_data)

# V.S. current data

tbl(con, I("test_lake.customers")) |> collect()
# ==============================================================================
# Step 4: Schema Evolution
# ==============================================================================

cli_h2("Schema Evolution - Adding a column")
DBI::dbExecute(
  con,
  "ALTER TABLE test_lake.customers ADD COLUMN status VARCHAR DEFAULT 'active'"
)
cli_alert_success("Added 'status' column")

customers_evolved <- tbl(con, I("test_lake.customers")) |> collect()
print(customers_evolved)

# ==============================================================================
# Step 5: MERGE INTO (Upsert)
# ==============================================================================

cli_h2("MERGE INTO - Upsert functionality")

# Create source table with updates
DBI::dbExecute(
  con,
  "
    CREATE TEMP TABLE updates AS
    SELECT * FROM (VALUES
        (1, 'Alice', 'alice.updated@example.com', 'vip'),
        (5, 'Eve', 'eve@example.com', 'new')
    ) AS t(id, name, email, status)
"
)

DBI::dbExecute(
  con,
  "
    MERGE INTO test_lake.customers AS target
    USING updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        email = source.email,
        status = source.status
    WHEN NOT MATCHED THEN INSERT (id, name, email, status)
        VALUES (source.id, source.name, source.email, source.status)
"
)
cli_alert_success("Merged updates (Alice updated, Eve inserted)")

final_data <- tbl(con, I("test_lake.customers")) |>
  arrange(id) |>
  collect()
print(final_data)

# ==============================================================================
# Step 6: Show Metadata
# ==============================================================================

cli_h2("Metadata inspection")

# The catalog is called 'test_lake' (from ATTACH ... AS test_lake)
catalog <- "test_lake"

cli_alert_info("Snapshots (version history):")
all_snapshots <- DBI::dbGetQuery(
  con,
  sprintf("SELECT * FROM ducklake_snapshots('%s')", catalog)
)
print(all_snapshots)

cli_alert_info("Tables in DuckLake:")
tables <- DBI::dbGetQuery(
  con,
  sprintf("SELECT * FROM ducklake_table_info('%s')", catalog)
)
print(tables)

# ==============================================================================
# Summary
# ==============================================================================

cli_h1("Key Takeaways")
cli_ol(c(
  "Metadata in SQL database (simple, queryable)",
  "Data stored as Parquet files (efficient, portable)",
  "Time Travel for auditing and recovery",
  "ACID transactions for data integrity",
  "Schema Evolution without rewriting data",
  "MERGE INTO for easy upserts"
))
cli_text("")
cli_alert_success(
  "Perfect for: Data lakes that need versioning + transactions
without the complexity of Iceberg/Delta Lake!"
)

cli_h1("Demo complete!")

# ==============================================================================
# Cleanup
# ==============================================================================

# Detach the DuckLake database
DBI::dbExecute(con, "DETACH test_lake")

# Close the connection explicitly (will need to restart R for new duckplyr operations)
DBI::dbDisconnect(con, shutdown = TRUE)

# To fully clean up demo files, uncomment:
unlink(path, recursive = TRUE)
