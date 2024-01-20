CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS request_logs;
DROP TABLE IF EXISTS spot_klines_1d;

CREATE TABLE spot_klines_1d (
	symbol TEXT NOT NULL,
	date   TIMESTAMP NOT NULL,
	open   DECIMAL,
	high   DECIMAL,
	low    DECIMAL,
	close  DECIMAL,
	volume DECIMAL,
	close_time TIMESTAMP,
	quote_volume   DECIMAL,
	trade_count    INTEGER,
	buy_volume     DECIMAL,
	buy_quote_volume DECIMAL
);
SELECT create_hypertable('spot_klines_1d', by_range('date'));
CREATE INDEX ix_symbol_time ON spot_klines_1d (symbol, date DESC);

CREATE TABLE request_logs (
	ip    TEXT NOT NULL,
	time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	sql   TEXT NOT NULL,
	cells INT  NOT NULL,
	err   TEXT NOT NULL
);
SELECT create_hypertable('request_logs', by_range('time'));
CREATE INDEX ix_ip_time ON request_logs (ip, time DESC);

DROP USER IF EXISTS visitor;
CREATE USER visitor WITH PASSWORD '123456';
GRANT SELECT ON spot_klines_1d TO visitor;
GRANT SELECT ON request_logs TO visitor;
GRANT INSERT ON request_logs TO visitor;
