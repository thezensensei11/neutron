# Neutron Data Pipeline Code Review

## Overview
I have conducted a comprehensive review of the data pipeline, covering the Downloader, Aggregator, Synthetic Service, Resampler, and Data Processor.

## Critical Findings

### 1. Synthetic Data Gap Handling (Critical)
In `src/neutron/services/synthetic.py`, gaps in data (where neither spot nor swap data exists) are currently filled with `0`:
```python
result.fillna(0, inplace=True)
```
**Issue**: This causes price columns (Open, High, Low, Close) to drop to 0 during gaps, which will severely distort charts and technical indicators.
**Recommendation**: Use **Forward Fill** (`ffill`) for price columns to propagate the last known price, and fill `0` only for Volume.

### 2. Resampler Memory Usage (Scalability)
In `src/neutron/services/resampler.py`, the service loads **all** 1-minute data for an asset into memory before resampling:
```python
full_df = pd.concat(dfs)
```
**Issue**: For assets with long histories (e.g., BTC since 2017), this involves loading millions of rows. While manageable now (~500MB), it will lead to Out-Of-Memory (OOM) errors as data grows or with more granular data.
**Recommendation**: Implement **chunked processing** (e.g., year-by-year or month-by-month) to keep memory usage constant.

## Minor Improvements

### 3. Aggregator Timezone Handling
In `src/neutron/services/aggregator.py`, timezone conversion is applied:
```python
result['time'] = result['time'].dt.tz_convert(timezone.utc)
```
**Observation**: If the source dataframe is timezone-naive (which can happen if `pd.read_parquet` doesn't preserve metadata correctly or if source was saved naively), this line will raise an error.
**Recommendation**: Use `tz_localize('UTC')` if naive, else `tz_convert('UTC')`.

### 4. Downloader Configuration Passing
In `src/neutron/core/downloader.py`, the configuration is passed as a dictionary constructed on the fly:
```python
agg_config = {'storage': {...}, 'max_workers': ...}
```
**Observation**: This decouples the service from the main config object, which is good for testing but requires manual updates if new config parameters are added.
**Recommendation**: Consider passing the full config object or a dedicated config model.

### 4. Exchange Network Robustness
In `src/neutron/exchange/binance.py` (and potentially the base `CCXTExchange`), `fetch_ohlcv` relies on the underlying library's default behavior.
**Observation**: Long-running backfills are susceptible to transient network errors.
**Recommendation**: Add explicit `tenacity` retry decorators to `fetch_ohlcv` to handle `NetworkError` and `RateLimitExceeded` automatically.

### 4. Configuration Consistency
In `src/neutron/core/config.py`, `StorageConfig` is missing the `aggregated_path` field, although `DataCrawler` handles it.
**Recommendation**: Add `aggregated_path` to `StorageConfig` for type safety and clarity.

### 5. Database Schema Uniqueness
In `src/neutron/db/models.py`, the `OHLCV` table's primary key is `(time, symbol, exchange, timeframe)`.
**Issue**: It is missing `instrument_type`. If a symbol exists as both Spot and Swap (e.g., `BTC/USDT`), this could lead to primary key collisions.
**Recommendation**: Add `instrument_type` to the `OHLCV` composite primary key.

## Action Plan
1.  **Fix Synthetic Gap Handling**: Update `SyntheticOHLCVService` to use `ffill` for prices.
2.  **Optimize Resampler**: Refactor `ResamplerService` to process data in chunks (e.g., yearly).
3.  **Harden Timezone Logic**: Ensure robust timezone handling in `AggregatorService`.
4.  **Update DB Schema**: Add `instrument_type` to `OHLCV` primary key.
5.  **Update Config**: Add `aggregated_path` to `StorageConfig`.
