
CREATE SCHEMA IF NOT EXISTS tink;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS sys_upd;
CREATE SCHEMA IF NOT EXISTS anl;
CREATE DATABASE airflow;

DROP TABLE IF EXISTS tink.security;
CREATE TABLE IF NOT EXISTS tink.security (
    "figi" TEXT, 
    "ticker" TEXT, 
    "isin" TEXT, 
    "minPriceIncrement" NUMERIC, 
    "lot" NUMERIC,
    "currency" TEXT, 
    "name" TEXT, 
    "type" TEXT,
    PRIMARY KEY("figi", "ticker")
    );

DROP TABLE IF EXISTS tink.candles_day;
CREATE TABLE IF NOT EXISTS tink.candle_day (
    "o" NUMERIC,
    "c" NUMERIC,
    "h" NUMERIC,
    "l" NUMERIC,
    "v" NUMERIC,
    "time" TIMESTAMP WITH TIME ZONE,
    "figi" TEXT,
    PRIMARY KEY("figi", "time")
    );

DROP TABLE IF EXISTS tink.candles_hour;
CREATE TABLE IF NOT EXISTS tink.candle_hour (
    "o" NUMERIC,
    "c" NUMERIC,
    "h" NUMERIC,
    "l" NUMERIC,
    "v" NUMERIC,
    "time" TIMESTAMP WITH TIME ZONE,
    "figi" TEXT,
    PRIMARY KEY("figi", "time")
    );

DROP TABLE IF EXISTS sys_upd.tink_candle;
CREATE TABLE IF NOT EXISTS sys_upd.tink_candle (
    "figi" TEXT,
    "ticker" TEXT,
    "interval" TEXT,
    "last_update" TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY("figi", "ticker", "interval"),
    FOREIGN KEY("figi", "ticker") REFERENCES tink.security("figi", "ticker") ON DELETE CASCADE);



CREATE OR REPLACE FUNCTION get_txt_date(date TEXT)
RETURNS DATE AS $$
BEGIN
	RETURN
	TO_DATE(
		SUBSTRING(
			SPLIT_PART(date, '-', 1) || ';' ||
			SPLIT_PART(date, '-', 2) || ';' ||
			CASE
				WHEN
					SPLIT_PART(date, '-', 3) = '31' THEN '30'
				ELSE
					SPLIT_PART(date, '-', 3) END,
		'[0-9]{4};[0-9]{2};[0-9]{2}'), 'YYYY;MM;DD');
END;
$$
LANGUAGE PLPGSQL;