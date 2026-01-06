# ============================================================
# DuckLake Demo - 5 Minute Presentation
# ============================================================
# Shows: Metadata in SQL, Time Travel, ACID, Schema Evolution, MERGE INTO

library(duckplyr)

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
DBI::dbGetQuery(con, "SELECT * FROM duckdb_extensions() WHERE extension_name = 'ducklake'")

cat("============================================================\n")
cat("DuckLake Demo - Lakehouse directly in DuckDB\n")
cat("============================================================\n")

# --- 1. CREATE A DUCKLAKE DATABASE ---
cat("\n1. Creating DuckLake database (metadata in SQLite, data in Parquet)\n")
DBI::dbExecute(con, sprintf("ATTACH 'ducklake:%s/metadata.ducklake' AS test_lake", path)) # Schema name is 'test_lake'
cat("   DuckLake attached - metadata stored in 'metadata.ducklake'\n")
# system("file metadata_files/metadata.ducklake")
# system("file metadata_files/metadata.ducklake.wal")

#Verify attachment
DBI::dbGetQuery(con, "SHOW DATABASES")
file.exists(file.path(path, "metadata.ducklake"))

# --- 2. CREATE A TABLE AND INSERT DATA ---
cat("\n2. Creating table and inserting data\n")
DBI::dbExecute(con, "
    CREATE TABLE test_lake.customers (
        id INTEGER,
        name VARCHAR,
        email VARCHAR
    )
")

DBI::dbExecute(con, "
    INSERT INTO test_lake.customers (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com')
")
cat("   Inserted 3 customers\n")

# Query with duckplyr
cat("\n   Current data:\n")
customers <- tbl(con, I("test_lake.customers")) |> collect()
print(customers)

# --- 3. TIME TRAVEL ---
cat("\n3. Time Travel Demo\n")

# Get snapshot AFTER initial insert (this is the version with our 3 customers)
snapshots <- DBI::dbGetQuery(con, "SELECT * FROM ducklake_snapshots('test_lake')")
cat("   Current snapshots (after initial insert):\n")
print(snapshots)

# Save the latest snapshot version (with 3 customers)
snapshot_before_changes <- max(snapshots$snapshot_id)
cat(sprintf("   Saving snapshot version %s as our 'before' state\n", snapshot_before_changes))

# Make changes
DBI::dbExecute(con, "UPDATE test_lake.customers SET email = 'alice.new@example.com' WHERE id = 1")
DBI::dbExecute(con, "INSERT INTO test_lake.customers (id, name, email) VALUES (4, 'Diana', 'diana@example.com')")
cat("\n   Updated Alice's email and added Diana\n")

cat("\n   Data AFTER changes:\n")
customers_after <- tbl(con, I("test_lake.customers")) |> collect()
print(customers_after)

# Time travel back to see original data
cat(sprintf("\n   Time travel to version %s (BEFORE changes):\n", snapshot_before_changes))
old_data <- DBI::dbGetQuery(con, sprintf(
  "SELECT * FROM test_lake.customers AT (VERSION => %s)", snapshot_before_changes
))
print(old_data)

# --- 4. SCHEMA EVOLUTION ---
cat("\n4. Schema Evolution - Adding a column\n")
DBI::dbExecute(con, "ALTER TABLE test_lake.customers ADD COLUMN status VARCHAR DEFAULT 'active'")
cat("   Added 'status' column\n")

customers_evolved <- tbl(con, I("test_lake.customers")) |> collect()
print(customers_evolved)

# --- 5. MERGE INTO (Upsert) ---
cat("\n5. MERGE INTO - Upsert functionality\n")

# Create source table with updates
DBI::dbExecute(con, "
    CREATE TEMP TABLE updates AS
    SELECT * FROM (VALUES
        (1, 'Alice', 'alice.updated@example.com', 'vip'),
        (5, 'Eve', 'eve@example.com', 'new')
    ) AS t(id, name, email, status)
")

DBI::dbExecute(con, "
    MERGE INTO test_lake.customers AS target
    USING updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        email = source.email,
        status = source.status
    WHEN NOT MATCHED THEN INSERT (id, name, email, status)
        VALUES (source.id, source.name, source.email, source.status)
")
cat("   Merged updates (Alice updated, Eve inserted)\n")

final_data <- tbl(con, I("test_lake.customers")) |>
  arrange(id) |>
  collect()
print(final_data)

# --- 6. SHOW METADATA ---
cat("\n6. Metadata inspection\n")
cat("\n   Snapshots (version history):\n")
all_snapshots <- DBI::dbGetQuery(con, "SELECT * FROM ducklake_snapshots('lake')")
print(all_snapshots)

cat("\n   Tables in DuckLake:\n")
tables <- DBI::dbGetQuery(con, "SELECT * FROM ducklake_tables('lake')")
print(tables)

# --- SUMMARY ---
cat("\n============================================================\n")
cat("Key Takeaways:\n")
cat("============================================================\n")
cat("
1. Metadata in SQL database (simple, queryable)
2. Data stored as Parquet files (efficient, portable)
3. Time Travel for auditing and recovery
4. ACID transactions for data integrity
5. Schema Evolution without rewriting data
6. MERGE INTO for easy upserts

Perfect for: Data lakes that need versioning + transactions
without the complexity of Iceberg/Delta Lake!
\n")

cat("Demo complete!\n")

# --- CLEANUP ---
# Detach the DuckLake database
DBI::dbExecute(con, "DETACH test_lake")

# Close the connection explicitly (will need to restart R for new duckplyr operations)
DBI::dbDisconnect(con, shutdown = TRUE)

# To fully clean up demo files, uncomment:
# unlink(path, recursive = TRUE)
