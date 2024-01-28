CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS request_logs;
DROP TABLE IF EXISTS spot_klines_1d;
DROP TABLE IF EXISTS spot_symbols_info;

CREATE TABLE spot_klines_1d (
	symbol TEXT NOT NULL,
	open_time   TIMESTAMP NOT NULL,
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

CREATE TABLE spot_symbols_info (
	symbol TEXT UNIQUE,
	quote_asset TEXT NOT NULL,
	base_asset TEXT NOT NULL,
	quote_asset_precision INTEGER NOT NULL,
	base_asset_precision INTEGER NOT NULL,
	quote_fee_precision INTEGER NOT NULL,
	base_fee_precision INTEGER NOT NULL,

	iceberg_allowed BOOLEAN NOT NULL,
	oco_allowed BOOLEAN NOT NULL,
	quote_order_qty_market_allowed BOOLEAN NOT NULL,
	is_spot_trading_allowed BOOLEAN NOT NULL,
	is_margin_trading_allowed BOOLEAN NOT NULL,

	created_at TIMESTAMP NOT NULL,
	updated_at TIMESTAMP NOT NULL
);

DROP USER IF EXISTS visitor;
CREATE USER visitor WITH PASSWORD '123456';
GRANT SELECT ON spot_klines_1d TO visitor;
GRANT SELECT ON request_logs TO visitor;
GRANT INSERT ON request_logs TO visitor;
GRANT SELECT ON spot_symbols_info TO visitor;
