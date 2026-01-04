
CREATE DATABASE stock_management;
USE DATABASE stock_management;
CREATE SCHEMA inventory;
USE SCHEMA inventory;

-- Create your main stock table
CREATE TABLE daily_stock (
    record_date DATE,
    location VARCHAR(100),
    item_name VARCHAR(200),
    opening_stock NUMBER(10,2),
    received NUMBER(10,2),
    issued NUMBER(10,2),
    closing_stock NUMBER(10,2),
    lead_time_days NUMBER(3),
    PRIMARY KEY (record_date, location, item_name)
);

INSERT INTO daily_stock VALUES
('2026-01-01', 'Hospital A', 'Paracetamol 500mg', 1000, 0, 150, 850, 7),
('2026-01-02', 'Hospital A', 'Paracetamol 500mg', 850, 0, 200, 650, 7),
('2026-01-03', 'Hospital A', 'Paracetamol 500mg', 650, 0, 180, 470, 7),
('2026-01-01', 'Hospital A', 'Insulin 100IU', 200, 0, 25, 175, 14),
('2026-01-02', 'Hospital A', 'Insulin 100IU', 175, 0, 30, 145, 14),
('2026-01-01', 'Clinic B', 'Paracetamol 500mg', 500, 0, 80, 420, 5),
('2026-01-02', 'Clinic B', 'Paracetamol 500mg', 420, 0, 90, 330, 5);

SELECT * 
FROM daily_stock 
ORDER BY record_date, location, item_name;

USE WAREHOUSE COMPUTE_WH;

CREATE DYNAMIC TABLE stock_health_metrics
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT 
    location,
    item_name,
    MAX(record_date) AS last_update_date,
    AVG(issued) AS avg_daily_usage,
    MAX(closing_stock) AS current_stock,
    MAX(lead_time_days) AS lead_time_days,
    CASE 
        WHEN AVG(issued) > 0 
        THEN ROUND(MAX(closing_stock) / AVG(issued), 1)
        ELSE 999 
    END AS days_until_stockout,
    CASE 
        WHEN MAX(closing_stock) / NULLIF(AVG(issued), 0) < MAX(lead_time_days) THEN 'CRITICAL'
        WHEN MAX(closing_stock) / NULLIF(AVG(issued), 0) < (MAX(lead_time_days) * 1.5) THEN 'LOW'
        WHEN MAX(closing_stock) / NULLIF(AVG(issued), 0) > (MAX(lead_time_days) * 4) THEN 'OVERSTOCK'
        ELSE 'HEALTHY'
    END AS stock_status
FROM daily_stock
WHERE record_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY location, item_name;

SELECT * 
FROM stock_health_metrics
ORDER BY days_until_stockout;

CREATE OR REPLACE DYNAMIC TABLE reorder_recommendations
TARGET_LAG = '1 hour'
WAREHOUSE = COMPUTE_WH
AS
SELECT 
    location,
    item_name,
    current_stock,
    avg_daily_usage,
    days_until_stockout,
    lead_time_days,
    stock_status,
    GREATEST(
        0,
        ROUND((avg_daily_usage * lead_time_days * 2) - current_stock, 0)
    ) AS recommended_reorder_qty,
    CASE 
        WHEN stock_status = 'CRITICAL' THEN 1
        WHEN stock_status = 'LOW' THEN 2
        WHEN stock_status = 'OVERSTOCK' THEN 4
        ELSE 3
    END AS priority
FROM stock_health_metrics
WHERE stock_status IN ('CRITICAL', 'LOW');


SELECT * 
FROM reorder_recommendations
ORDER BY priority, days_until_stockout;

-- Create alert check procedure
CREATE OR REPLACE PROCEDURE check_critical_stock()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    LET critical_count NUMBER;
    
    SELECT COUNT(*) INTO :critical_count
    FROM stock_health_metrics
    WHERE stock_status = 'CRITICAL';
    
    IF (critical_count > 0) THEN
        RETURN 'ALERT: ' || critical_count || ' critical items found';
    ELSE
        RETURN 'All stock levels healthy';
    END IF;
END;
$$;
CALL check_critical_stock();

-- Schedule daily check
-- Create a stream to monitor changes in stock health
CREATE STREAM stock_health_changes 
ON TABLE stock_health_metrics
APPEND_ONLY = FALSE;

-- Create table to log alerts
CREATE TABLE stock_alerts (
    alert_id NUMBER AUTOINCREMENT,
    alert_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    location VARCHAR(100),
    item_name VARCHAR(200),
    stock_status VARCHAR(20),
    days_until_stockout NUMBER(10,1),
    current_stock NUMBER(10,2),
    alert_message VARCHAR(500)
);

-- Create task that processes stream changes
CREATE OR REPLACE TASK process_stock_changes
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('stock_health_changes')
AS
INSERT INTO stock_alerts (location, item_name, stock_status, 
                          days_until_stockout, current_stock, alert_message)
SELECT 
    location,
    item_name,
    stock_status,
    days_until_stockout,
    current_stock,
    CASE 
        WHEN stock_status = 'CRITICAL' 
        THEN 'URGENT: Stock critically low at ' || location || ' for ' || item_name
        WHEN stock_status = 'LOW' 
        THEN 'WARNING: Stock running low at ' || location || ' for ' || item_name
        ELSE 'INFO: Stock status changed for ' || item_name
    END as alert_message
FROM stock_health_changes
WHERE METADATA$ACTION = 'INSERT' 
  AND stock_status IN ('CRITICAL', 'LOW');

-- Resume the task
ALTER TASK process_stock_changes RESUME;

-- View alerts
SELECT * FROM stock_alerts ORDER BY alert_timestamp DESC LIMIT 10;
SHOW TASKS LIKE 'PROCESS_STOCK_CHANGES';

-- sample
UPDATE stock_health_snapshot
SET current_stock = current_stock - 100
WHERE location = 'Hospital A'
  AND item_name = 'Paracetamol 500mg';

EXECUTE TASK process_stock_changes;
SELECT * 
FROM stock_alerts 
ORDER BY alert_timestamp DESC;

-- First, create a procedure using Snowpark Python
CREATE OR REPLACE PROCEDURE forecast_demand(
    location_name STRING, 
    item_name STRING
)
RETURNS TABLE (
    FORECAST_DATE DATE,
    PREDICTED_USAGE NUMBER(10,2)
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'forecast_handler'
AS
$$
from datetime import timedelta

def forecast_handler(session, location_name, item_name):
    df = session.sql(f"""
        SELECT record_date, issued
        FROM daily_stock
        WHERE location = '{location_name}'
        AND item_name = '{item_name}'
        ORDER BY record_date
    """).to_pandas()

    # Case 1: Not enough data → return EMPTY but TYPED dataframe
    if len(df) < 7:
        return session.create_dataframe(
            [],
            schema="FORECAST_DATE DATE, PREDICTED_USAGE NUMBER(10,2)"
        )

    avg = round(df['ISSUED'].tail(7).mean(), 2)
    last_date = df['RECORD_DATE'].max()

    forecast = [
        (last_date + timedelta(days=i), avg)
        for i in range(1, 15)
    ]

    return session.create_dataframe(
        forecast,
        schema="FORECAST_DATE DATE, PREDICTED_USAGE NUMBER(10,2)"
    )
$$;


-- Test the forecast
CALL forecast_demand('Hospital A', 'Paracetamol 500mg');
INSERT INTO daily_stock VALUES
('2026-01-04','Hospital A','Paracetamol 500mg',470,0,160,310,7),
('2026-01-05','Hospital A','Paracetamol 500mg',310,0,155,155,7),
('2026-01-06','Hospital A','Paracetamol 500mg',155,0,150,5,7),
('2026-01-07','Hospital A','Paracetamol 500mg',5,500,120,385,7),
('2026-01-08','Hospital A','Paracetamol 500mg',385,0,140,245,7);

CREATE OR REPLACE TABLE demand_forecasts (
    location STRING,
    item_name STRING,
    forecast_date DATE,
    predicted_usage NUMBER,
    generated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK refresh_forecasts
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
INSERT INTO demand_forecasts (location, item_name, forecast_date, predicted_usage)
SELECT
    m.location,
    m.item_name,
    f.forecast_date,
    f.predicted_usage
FROM stock_health_metrics m,
     TABLE(forecast_demand(m.location, m.item_name)) f;
ALTER TASK refresh_forecasts RESUME;


