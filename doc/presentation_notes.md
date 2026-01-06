# DuckLake Presentation Notes (5 min)

## Opening (30 sec)
"I want to show you DuckLake - a new DuckDB extension from May 2025 that brings lakehouse capabilities directly into DuckDB. This could be relevant for some projects where we need to manage metadata for data lake files."

## What is DuckLake? (1 min)
- **The problem**: Traditional data lakes (just Parquet files) have no versioning, no transactions, no easy way to track changes
- **Existing solutions**: Iceberg and Delta Lake solve this, but they're complex - metadata stored in JSON/Avro files, need Spark clusters
- **DuckLake's approach**: Store metadata in a simple SQL database (SQLite), keep data in Parquet files
  - "It's like having a Git-like version control for your data, but queryable with SQL"

## Key Features (2.5 min - during demo)

### 1. Simple Setup
- Just `ATTACH 'ducklake:metadata.ducklake'` - that's it
- No complex infrastructure, no Spark, no Java

### 2. Time Travel
- "Every change creates a snapshot"
- Query historical data: `SELECT * FROM table AT (SNAPSHOT => 123)`
- Use case: "What did the data look like yesterday before that ETL job ran?"

### 3. ACID Transactions
ACID = Atomicity, Consistency, Isolation, Durability
- **Atomicity**: All or nothing - if any part fails, entire transaction rolls back
- **Consistency**: Database always moves from one valid state to another
- **Isolation**: Concurrent transactions don't interfere with each other
- **Durability**: Once committed, data survives crashes

**Why this matters for data lakes:**
Plain Parquet files have no transaction support. If you're writing multiple files and the process crashes halfway, you end up with a "half-updated" data lake - some files are new, some are old, and your data is inconsistent.

DuckLake wraps operations in transactions, so:
- Multi-table updates are atomic
- Failed writes don't leave partial data
- Concurrent users see consistent snapshots

"This is why ACID matters for data lakes - it prevents corrupted or inconsistent states."

### 4. Schema Evolution
- Add/remove/change columns without rewriting all data
- `ALTER TABLE ADD COLUMN` just updates metadata

### 5. MERGE INTO (Upsert)
- Update existing rows OR insert new ones in one statement
- Perfect for incremental data loads
- "No more DELETE + INSERT workarounds"

## Closing (30 sec)
- DuckLake = lakehouse features with DuckDB simplicity
- Metadata queryable with SQL (no parsing JSON manifests)
- Worth evaluating for projects needing light-weight data versioning

## Links
- https://duckdb.org/docs/stable/core_extensions/ducklake
- https://duckdb.org/2025/05/27/ducklake
- https://ducklake.select/manifesto/
