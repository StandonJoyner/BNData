CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS request_logs;
DROP TABLE IF EXISTS spot_klines_1d;

CREATE TABLE spot_klines_1d (
	symbol TEXT NOT NULL,
	open_time TIMESTAMP NOT NULL,
	open   DECIMAL NOT NULL,
	high   DECIMAL NOT NULL,
	low    DECIMAL NOT NULL,
	close  DECIMAL NOT NULL,
	volume DECIMAL NOT NULL,
	close_time TIMESTAMP NOT NULL,
	quote_volume   DECIMAL NOT NULL,
	trade_count    INTEGER NOT NULL,
	buy_volume     DECIMAL NOT NULL,
	buy_quote_volume DECIMAL NOT NULL
);
SELECT create_hypertable('spot_klines_1d', by_range('open_time'));
CREATE INDEX ix_symbol_time ON spot_klines_1d (symbol, open_time DESC);

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
