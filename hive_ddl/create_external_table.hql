-- Reference DDL: what the lake/transactions/ directory this demo produces would
-- look like as a real Hive external table on an actual cluster. Not executed by
-- this demo (there's no real Hive metastore here) -- included to show the schema
-- design this pipeline is targeting, matching the "Component roles" and
-- "Schema & file format" sections of the architecture proposal.

CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    transaction_id  BIGINT,
    account_id      STRING,
    merchant_id     STRING,
    amount          DECIMAL(12,2),
    currency        STRING,
    transaction_ts  TIMESTAMP,
    status          STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/data/lake/transactions';

-- After adding new partition directories (as migrate_to_lake.py / incremental_sync.py do):
MSCK REPAIR TABLE transactions;

-- Example query that benefits directly from the partitioning + bucketing design --
-- Hive prunes to just the needed month partitions instead of scanning everything:
-- SELECT merchant_id, COUNT(*), SUM(amount)
-- FROM transactions
-- WHERE year = 2024 AND month = 6
-- GROUP BY merchant_id;

-- Storage-tiering note (see proposal, Section 5): in production, partitions older
-- than the active window would have their HDFS storage policy set to an
-- Erasure Coding policy (e.g. RS-6-3) instead of the default 3x replication:
-- hdfs ec -setPolicy -path /data/lake/transactions/year=2024/month=01 -policy RS-6-3-1024k
