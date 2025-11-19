# PostgreSQL Data Monitoring Queries

## Database Info
- Database: `reddit_miner_db`
- Table: `dataentity`
- Source 1: Reddit
- Source 2: Twitter/X

## Quick Status Checks

### Count total records by source
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(*) as total_records
FROM dataentity 
GROUP BY source 
ORDER BY source;
```

### Count records added in last 30 minutes
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(*) as records_added
FROM dataentity 
WHERE datetime >= NOW() - INTERVAL '30 minutes'
GROUP BY source 
ORDER BY source;
```

### Total records added in last 30 minutes (all sources)
```sql
SELECT COUNT(*) as total_records_last_30min
FROM dataentity 
WHERE datetime >= NOW() - INTERVAL '30 minutes';
```

## Detailed Statistics

### Records by source with size information
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(*) as total_records,
    SUM(contentsizebytes) as total_bytes,
    ROUND(SUM(contentsizebytes)::numeric / 1024 / 1024, 2) as total_mb,
    ROUND(AVG(contentsizebytes)::numeric, 2) as avg_bytes_per_record
FROM dataentity 
GROUP BY source 
ORDER BY source;
```

### Records added in last hour (by source)
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(*) as records_last_hour
FROM dataentity 
WHERE datetime >= NOW() - INTERVAL '1 hour'
GROUP BY source 
ORDER BY source;
```

### Records added by time intervals
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '10 minutes' THEN 1 END) as last_10min,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '30 minutes' THEN 1 END) as last_30min,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '1 hour' THEN 1 END) as last_1hour,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '24 hours' THEN 1 END) as last_24hours
FROM dataentity 
GROUP BY source 
ORDER BY source;
```

## Recent Activity

### Last 20 records added (all sources)
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    datetime,
    LEFT(uri, 50) as uri_preview,
    contentsizebytes
FROM dataentity 
ORDER BY datetime DESC 
LIMIT 20;
```

### Latest record per source
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    MAX(datetime) as latest_record_time,
    NOW() - MAX(datetime) as time_since_last_record
FROM dataentity 
GROUP BY source 
ORDER BY source;
```

### Records per minute in last 30 minutes
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    DATE_TRUNC('minute', datetime) as minute,
    COUNT(*) as records_count
FROM dataentity 
WHERE datetime >= NOW() - INTERVAL '30 minutes'
GROUP BY source, DATE_TRUNC('minute', datetime)
ORDER BY minute DESC, source;
```

## Data Quality Checks

### Check for records without content
```sql
SELECT 
    source,
    COUNT(*) as records_without_content
FROM dataentity 
WHERE content IS NULL OR content = ''
GROUP BY source;
```

### Average record size by source
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    ROUND(AVG(contentsizebytes)::numeric, 2) as avg_bytes,
    MIN(contentsizebytes) as min_bytes,
    MAX(contentsizebytes) as max_bytes
FROM dataentity 
GROUP BY source 
ORDER BY source;
```

## Comprehensive Dashboard Query

### All stats in one query
```sql
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '30 minutes' THEN 1 END) as last_30min,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '1 hour' THEN 1 END) as last_1hour,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '24 hours' THEN 1 END) as last_24hours,
    ROUND(SUM(contentsizebytes)::numeric / 1024 / 1024, 2) as total_mb,
    MAX(datetime) as latest_record,
    NOW() - MAX(datetime) as time_since_last
FROM dataentity 
GROUP BY source 
ORDER BY source;
```

## Command Line Usage

### Run query from command line
```bash
# Basic query
psql -h localhost -U postgres -d reddit_miner_db -c "SELECT source, COUNT(*) FROM dataentity GROUP BY source;"

# With password (you'll be prompted)
PGPASSWORD=postgres psql -h localhost -U postgres -d reddit_miner_db -c "SELECT source, COUNT(*) FROM dataentity WHERE datetime >= NOW() - INTERVAL '30 minutes' GROUP BY source;"

# Save results to file
psql -h localhost -U postgres -d reddit_miner_db -c "SELECT * FROM dataentity WHERE datetime >= NOW() - INTERVAL '30 minutes';" > recent_data.txt

# Get formatted output
psql -h localhost -U postgres -d reddit_miner_db -x -c "SELECT source, COUNT(*) as count FROM dataentity GROUP BY source;"
```

### Quick one-liners

```bash
# Count by source
echo "SELECT source, COUNT(*) FROM dataentity GROUP BY source;" | PGPASSWORD=postgres psql -h localhost -U postgres -d reddit_miner_db

# Last 30 minutes
echo "SELECT source, COUNT(*) FROM dataentity WHERE datetime >= NOW() - INTERVAL '30 minutes' GROUP BY source;" | PGPASSWORD=postgres psql -h localhost -U postgres -d reddit_miner_db

# Total records
echo "SELECT COUNT(*) as total FROM dataentity;" | PGPASSWORD=postgres psql -h localhost -U postgres -d reddit_miner_db
```

## Create Monitoring Script

```bash
cat > check_db_stats.sh << 'EOF'
#!/bin/bash

echo "======================================"
echo "Database Statistics"
echo "======================================"
echo ""

PGPASSWORD=postgres psql -h localhost -U postgres -d reddit_miner_db << SQL
SELECT 
    source,
    CASE 
        WHEN source = 1 THEN 'Reddit'
        WHEN source = 2 THEN 'Twitter/X'
        ELSE 'Unknown'
    END as source_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '30 minutes' THEN 1 END) as last_30min,
    COUNT(CASE WHEN datetime >= NOW() - INTERVAL '1 hour' THEN 1 END) as last_1hour,
    MAX(datetime) as latest_record
FROM dataentity 
GROUP BY source 
ORDER BY source;
SQL

echo ""
echo "======================================"
EOF

chmod +x check_db_stats.sh
```

Then run: `./check_db_stats.sh`

## Watch Mode (Auto-refresh)

```bash
# Refresh every 5 seconds
watch -n 5 "echo 'SELECT source, COUNT(*) FROM dataentity WHERE datetime >= NOW() - INTERVAL '\''30 minutes'\'' GROUP BY source;' | PGPASSWORD=postgres psql -h localhost -U postgres -d reddit_miner_db"
```

## Useful Time Intervals

Replace `'30 minutes'` with:
- `'10 minutes'` - Last 10 minutes
- `'1 hour'` - Last hour
- `'6 hours'` - Last 6 hours
- `'24 hours'` or `'1 day'` - Last 24 hours
- `'7 days'` or `'1 week'` - Last week
- `'30 days'` or `'1 month'` - Last month
