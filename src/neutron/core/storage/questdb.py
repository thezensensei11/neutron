import logging
import socket
import threading
import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from .base import StorageBackend, DataQualityReport

logger = logging.getLogger(__name__)

class QuestDBStorage(StorageBackend):
    def __init__(self, host: str = 'localhost', ilp_port: int = 9009, pg_port: int = 8812, database: str = 'qdb', username: str = 'admin', password: str = 'quest'):
        self.host = host
        self.ilp_port = ilp_port
        self.pg_port = pg_port
        self.database = database
        self.username = username
        self.password = password
        
        url = f"postgresql://{username}:{password}@{host}:{pg_port}/{database}"
        self.engine = create_engine(url)
        
        self._sock = None
        self._lock = threading.Lock()

    def _connect_ilp(self):
        if self._sock:
            return
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self.host, self.ilp_port))
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB ILP: {e}")
            self._sock = None

    def _send_ilp(self, lines: List[str], batch_size: int = 100):
        if not lines:
            return
        
        with self._lock:
            self._connect_ilp()
            if not self._sock:
                return

            total_lines = len(lines)
            for i in range(0, total_lines, batch_size):
                batch = lines[i:i + batch_size]
                try:
                    payload = '\n'.join(batch) + '\n'
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Sending batch {i//batch_size + 1} (size {len(batch)}). Sample: {batch[0][:100]}...")
                    self._sock.sendall(payload.encode('utf-8'))
                except Exception as e:
                    logger.warning(f"Error sending ILP data (batch {i//batch_size + 1}): {e}. Sample: {batch[0][:200]}... Retrying...")
                    self._sock.close()
                    self._sock = None
                    import time
                    time.sleep(1.0) # Wait before retry
                    try:
                        self._connect_ilp()
                        if self._sock:
                            self._sock.sendall(payload.encode('utf-8'))
                    except Exception as retry_e:
                        logger.error(f"Retry failed: {retry_e}")
                        pass

    def _escape_tag(self, tag: str) -> str:
        return str(tag).replace(' ', r'\ ').replace(',', r'\,').replace('=', r'\=')

    def _escape_string(self, s: str) -> str:
        return f'"{str(s).replace("\"", "\\\"").replace("\n", "\\n")}"'

    def _format_timestamp(self, dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1e9) # Nanoseconds

    def save_ohlcv(self, data: List[Dict[str, Any]]):
        if not data:
            return
        
        lines = []
        for row in data:
            measurement = 'ohlcv'
            tags = [
                f"exchange={self._escape_tag(row['exchange'])}",
                f"symbol={self._escape_tag(row['symbol'])}",
                f"timeframe={self._escape_tag(row['timeframe'])}",
                f"instrument_type={self._escape_tag(row.get('instrument_type', 'spot'))}"
            ]
            fields = [
                f"open={row['open']}",
                f"high={row['high']}",
                f"low={row['low']}",
                f"close={row['close']}",
                f"volume={row['volume']}",
                f"is_interpolated={'t' if row.get('is_interpolated') else 'f'}"
            ]
            timestamp = self._format_timestamp(row['time'])
            line = f"{measurement},{','.join(tags)} {','.join(fields)} {timestamp}"
            lines.append(line)
            
        self._send_ilp(lines)
        logger.debug(f"Sent {len(lines)} OHLCV records to QuestDB")

    def save_tick_data(self, data: List[Dict[str, Any]]):
        if not data:
            return
            
        lines = []
        for row in data:
            measurement = 'trades'
            tags = [
                f"exchange={self._escape_tag(row['exchange'])}",
                f"symbol={self._escape_tag(row['symbol'])}",
                f"instrument_type={self._escape_tag(row.get('instrument_type', 'spot'))}",
                f"side={self._escape_tag(row.get('side', 'unknown'))}"
            ]
            fields = [
                f"price={row['price']}",
                f"amount={row['amount']}",
                f"trade_id={self._escape_string(row['trade_id'])}"
            ]
            timestamp = self._format_timestamp(row['time'])
            line = f"{measurement},{','.join(tags)} {','.join(fields)} {timestamp}"
            lines.append(line)
            
        self._send_ilp(lines)
        logger.debug(f"Sent {len(lines)} tick records to QuestDB")

    def save_funding_rates(self, data: List[Dict[str, Any]]):
        if not data:
            return
            
        lines = []
        for row in data:
            measurement = 'funding_rates'
            tags = [
                f"exchange={self._escape_tag(row['exchange'])}",
                f"symbol={self._escape_tag(row['symbol'])}",
                f"instrument_type={self._escape_tag(row.get('instrument_type', 'swap'))}"
            ]
            fields = [
                f"rate={row['rate']}",
                f"mark_price={row.get('mark_price', 0.0)}"
            ]
            timestamp = self._format_timestamp(row['time'])
            line = f"{measurement},{','.join(tags)} {','.join(fields)} {timestamp}"
            lines.append(line)
            
        self._send_ilp(lines)
        logger.debug(f"Sent {len(lines)} funding rates to QuestDB")

    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        if not data:
            return
            
        lines = []
        for row in data:
            measurement = data_type
            tags = [
                f"exchange={self._escape_tag(row.get('exchange', 'unknown'))}",
                f"symbol={self._escape_tag(row.get('symbol', 'unknown'))}",
                f"instrument_type={self._escape_tag(row.get('instrument_type', 'spot'))}"
            ]
            fields = []
            for k, v in row.items():
                if k in ['exchange', 'symbol', 'instrument_type', 'time', 'create_time', 'open_time']:
                    continue
                if isinstance(v, str):
                    fields.append(f"{k}={self._escape_string(v)}")
                elif isinstance(v, (bool, np.bool_)):
                    fields.append(f"{k}={'t' if v else 'f'}")
                elif isinstance(v, (int, np.integer)):
                    fields.append(f"{k}={v}i")
                elif isinstance(v, (float, np.floating)):
                    fields.append(f"{k}={v}")
            
            if not fields:
                continue

            ts_val = row.get('time') or row.get('create_time') or row.get('open_time')
            if not ts_val:
                ts_val = datetime.now(timezone.utc)
            
            timestamp = self._format_timestamp(ts_val)
            line = f"{measurement},{','.join(tags)} {','.join(fields)} {timestamp}"
            lines.append(line)
            
        self._send_ilp(lines)
        logger.debug(f"Sent {len(lines)} {data_type} records to QuestDB")

    def _query_df(self, query: str, params: Dict = None) -> pd.DataFrame:
        try:
            if params:
                new_params = {}
                for k, v in params.items():
                    if isinstance(v, datetime):
                        if v.tzinfo:
                            v = v.astimezone(timezone.utc).replace(tzinfo=None)
                        new_params[k] = v.isoformat()
                    else:
                        new_params[k] = v
                params = new_params

            with self.engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return pd.DataFrame()

    def load_ohlcv(self, exchange: str, symbol: str, timeframe: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        query = """
            SELECT * FROM ohlcv 
            WHERE exchange = :exchange 
            AND symbol = :symbol 
            AND timeframe = :timeframe 
            AND timestamp >= :start_date 
            AND timestamp < :end_date
            ORDER BY timestamp
        """
        params = {
            'exchange': exchange, 
            'symbol': symbol, 
            'timeframe': timeframe, 
            'start_date': start_date, 
            'end_date': end_date
        }
        df = self._query_df(query, params)
        if not df.empty:
            df.rename(columns={'timestamp': 'time'}, inplace=True)
        return df

    def load_tick_data(self, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        query = """
            SELECT * FROM trades 
            WHERE exchange = :exchange 
            AND symbol = :symbol 
            AND timestamp >= :start_date 
            AND timestamp < :end_date
            ORDER BY timestamp
        """
        params = {
            'exchange': exchange, 
            'symbol': symbol, 
            'start_date': start_date, 
            'end_date': end_date
        }
        df = self._query_df(query, params)
        if not df.empty:
            df.rename(columns={'timestamp': 'time'}, inplace=True)
        return df

    def load_funding_rates(self, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'swap') -> pd.DataFrame:
        query = """
            SELECT * FROM funding_rates 
            WHERE exchange = :exchange 
            AND symbol = :symbol 
            AND timestamp >= :start_date 
            AND timestamp < :end_date
            ORDER BY timestamp
        """
        params = {
            'exchange': exchange, 
            'symbol': symbol, 
            'start_date': start_date, 
            'end_date': end_date
        }
        if not df.empty:
            df.rename(columns={'timestamp': 'time'}, inplace=True)
        return df

    def load_generic_data(self, data_type: str, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        if not data_type.replace('_', '').isalnum():
            logger.warning(f"Invalid table name: {data_type}")
            return pd.DataFrame()
            
        query = f"""
            SELECT * FROM "{data_type}"
            WHERE exchange = :exchange 
            AND symbol = :symbol 
            AND time >= :start_date 
            AND time < :end_date
            ORDER BY time
        """
        params = {
            'exchange': exchange, 
            'symbol': symbol, 
            'start_date': start_date, 
            'end_date': end_date
        }
        df = self._query_df(query, params)
        if not df.empty:
            df.rename(columns={'timestamp': 'time'}, inplace=True)
        return df

    def has_data(self, data_type: str, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> bool:
        """Check if data exists and covers the specified range."""
        # Sanitize table name
        if not data_type.replace('_', '').isalnum():
            return False
            
        query = f"""
            SELECT min(time), max(time) FROM "{data_type}"
            WHERE exchange = :exchange 
            AND symbol = :symbol 
            AND instrument_type = :instrument_type
            AND time >= :start_date 
            AND time < :end_date
        """
        params = {
            'exchange': exchange, 
            'symbol': symbol, 
            'instrument_type': instrument_type,
            'start_date': start_date, 
            'end_date': end_date
        }
        
        try:
            df = self._query_df(query, params)
            if not df.empty and df.iloc[0].iloc[0] is not None:
                min_ts = df.iloc[0].iloc[0]
                max_ts = df.iloc[0].iloc[1]
                
                # Ensure timezone awareness
                if min_ts.tzinfo is None: min_ts = min_ts.replace(tzinfo=timezone.utc)
                if max_ts.tzinfo is None: max_ts = max_ts.replace(tzinfo=timezone.utc)
                if start_date.tzinfo is None: start_date = start_date.replace(tzinfo=timezone.utc)
                if end_date.tzinfo is None: end_date = end_date.replace(tzinfo=timezone.utc)

                # Check if existing data covers the start and end (with some tolerance)
                # Tolerance of 1 hour for start/end to account for market gaps
                tolerance = timedelta(hours=1)
                
                starts_ok = min_ts <= (start_date + tolerance)
                ends_ok = max_ts >= (end_date - tolerance)
                
                return starts_ok and ends_ok
        except Exception as e:
            logger.warning(f"Error checking data existence: {e}")
            
        return False

    def list_available_data(self, deep_scan: bool = False) -> List[Dict[str, Any]]:
        results = []
        try:
            tables_df = self._query_df("SHOW TABLES")
            if tables_df.empty:
                return results
                
            tables = tables_df['table_name'].tolist()
            
            for table in tables:
                if table in ['telemetry', 'telemetry_config']: continue
                
                try:
                    cols_df = self._query_df(f"SHOW COLUMNS FROM \"{table}\"")
                    cols = set(cols_df['column'].tolist())
                    
                    if 'exchange' not in cols or 'symbol' not in cols:
                        continue
                        
                    sel_cols = ["exchange", "symbol"]
                    if 'instrument_type' in cols: sel_cols.append("first(instrument_type) as instrument_type")
                    else: sel_cols.append("'spot' as instrument_type")
                    
                    if 'timeframe' in cols: sel_cols.append("first(timeframe) as timeframe")
                    else: sel_cols.append("NULL as timeframe")
                    
                    query = f"""
                        SELECT 
                            {','.join(sel_cols)},
                            min(time) as start_date, 
                            max(time) as end_date, 
                            count() as count
                        FROM "{table}"
                        GROUP BY exchange, symbol
                    """
                    
                    summary_df = self._query_df(query)
                    
                    for _, row in summary_df.iterrows():
                        results.append({
                            'data_type': table, 
                            'exchange': row['exchange'],
                            'symbol': row['symbol'],
                            'instrument_type': row['instrument_type'],
                            'timeframe': row['timeframe'],
                            'start_date': row['start_date'],
                            'end_date': row['end_date'],
                            'count': row['count'],
                            'gaps': []
                        })
                        
                except Exception as e:
                    logger.warning(f"Could not summarize table {table}: {e}")
                    
        except Exception as e:
            logger.error(f"Error listing QuestDB tables: {e}")
            
        return results

    def analyze_ohlcv_quality(self, exchange: str, symbol: str, timeframe: str, instrument_type: str = 'spot') -> DataQualityReport:
        return DataQualityReport(
            exchange=exchange,
            symbol=symbol,
            instrument_type=instrument_type,
            data_type='ohlcv',
            timeframe=timeframe
        )

    def analyze_tick_quality(self, exchange: str, symbol: str, instrument_type: str = 'spot', data_type: str = 'tick') -> DataQualityReport:
        report = DataQualityReport(
            exchange=exchange,
            symbol=symbol,
            instrument_type=instrument_type,
            data_type=data_type
        )
        
        target_table = data_type if data_type in ['aggTrades', 'trades'] else 'aggTrades'
        
        try:
            query_stats = f"""
                SELECT 
                    min(time) as start_date,
                    max(time) as end_date,
                    count() as count
                FROM "{target_table}"
                WHERE exchange = :exchange AND symbol = :symbol
            """
            stats = self._query_df(query_stats, {'exchange': exchange, 'symbol': symbol})
            
            if not stats.empty:
                count_val = 0
                if 'count' in stats.columns:
                    count_val = stats.iloc[0]['count']
                elif 'count()' in stats.columns:
                    count_val = stats.iloc[0]['count()']
                
                if count_val > 0:
                    report.start_date = stats.iloc[0]['start_date']
                    report.end_date = stats.iloc[0]['end_date']
                    report.total_rows = int(count_val)
                    
                    gap_threshold_minutes = 60
                    query_gaps = f"""
                        SELECT time as timestamp, count() 
                        FROM "{target_table}"
                        WHERE exchange = :exchange AND symbol = :symbol
                        SAMPLE BY {gap_threshold_minutes}m FILL(0)
                    """
                    
                    buckets = self._query_df(query_gaps, {'exchange': exchange, 'symbol': symbol})
                    
                    if not buckets.empty:
                        count_col = 'count' if 'count' in buckets.columns else 'count()'
                        empty_buckets = buckets[buckets[count_col] == 0]
                        
                        if not empty_buckets.empty:
                            current_gap_start = None
                            current_gap_end = None
                            
                            for index, row in empty_buckets.iterrows():
                                ts = row['timestamp']
                                if current_gap_start is None:
                                    current_gap_start = ts
                                    current_gap_end = ts + pd.Timedelta(minutes=gap_threshold_minutes)
                                else:
                                    if ts == current_gap_end:
                                        current_gap_end += pd.Timedelta(minutes=gap_threshold_minutes)
                                    else:
                                        duration = current_gap_end - current_gap_start
                                        report.gaps.append((current_gap_start, current_gap_end, duration))
                                        
                                        current_gap_start = ts
                                        current_gap_end = ts + pd.Timedelta(minutes=gap_threshold_minutes)
                            
                            if current_gap_start:
                                duration = current_gap_end - current_gap_start
                                report.gaps.append((current_gap_start, current_gap_end, duration))

                    total_duration = (report.end_date - report.start_date).total_seconds()
                    gap_duration = sum([g[2].total_seconds() for g in report.gaps])
                    
                    if total_duration > 0:
                        report.quality_score = max(0, 100.0 * (1 - (gap_duration / total_duration)))
                    else:
                        report.quality_score = 100.0
                        
                    # --- Rich Analytics ---
                    # 1. Volume & Price Stats
                    try:
                        # Check columns first to avoid errors
                        cols_df = self._query_df(f"SHOW COLUMNS FROM \"{target_table}\"")
                        cols = set(cols_df['column'].tolist())
                        
                        qty_col = 'qty' if 'qty' in cols else ('amount' if 'amount' in cols else None)
                        price_col = 'price' if 'price' in cols else None
                        
                        if qty_col and price_col:
                            query_stats = f"""
                                SELECT 
                                    sum({qty_col}) as total_vol,
                                    avg({qty_col}) as avg_trade_size,
                                    min({price_col}) as min_price,
                                    max({price_col}) as max_price,
                                    avg({price_col}) as avg_price
                                FROM "{target_table}"
                                WHERE exchange = :exchange AND symbol = :symbol
                            """
                            stats_df = self._query_df(query_stats, {'exchange': exchange, 'symbol': symbol})
                            if not stats_df.empty:
                                row = stats_df.iloc[0]
                                report.volume_stats = {
                                    'total': float(row['total_vol']) if row['total_vol'] else 0.0,
                                    'avg_trade_size': float(row['avg_trade_size']) if row['avg_trade_size'] else 0.0
                                }
                                report.price_stats = {
                                    'min': float(row['min_price']) if row['min_price'] else 0.0,
                                    'max': float(row['max_price']) if row['max_price'] else 0.0,
                                    'avg': float(row['avg_price']) if row['avg_price'] else 0.0
                                }
                                report.trade_stats['count'] = report.total_rows
                                report.trade_stats['avg_size'] = report.volume_stats['avg_trade_size']

                        # 2. Buy/Sell Ratio
                        if 'is_buyer_maker' in cols:
                            query_bs = f"""
                                SELECT is_buyer_maker, count() 
                                FROM "{target_table}"
                                WHERE exchange = :exchange AND symbol = :symbol
                                GROUP BY is_buyer_maker
                            """
                            bs_df = self._query_df(query_bs, {'exchange': exchange, 'symbol': symbol})
                            if not bs_df.empty:
                                # Determine count column name
                                count_col = 'count' if 'count' in bs_df.columns else 'count()'
                                
                                # is_buyer_maker = true -> Sell (Maker is buyer)
                                # is_buyer_maker = false -> Buy (Taker is buyer)
                                buys = 0
                                sells = 0
                                for _, row in bs_df.iterrows():
                                    if row['is_buyer_maker']:
                                        sells = row[count_col]
                                    else:
                                        buys = row[count_col]
                                
                                total_trades = buys + sells
                                if total_trades > 0:
                                    report.trade_stats['buy_ratio'] = buys / total_trades
                                    report.trade_stats['sell_ratio'] = sells / total_trades
                                    
                    except Exception as e:
                        logger.warning(f"Error calculating rich analytics for {symbol}: {e}")

                    # --- Granular Metrics ---
                    # 3. Schema Info
                    try:
                        cols_df = self._query_df(f"SHOW COLUMNS FROM \"{target_table}\"")
                        if not cols_df.empty:
                            for _, row in cols_df.iterrows():
                                report.schema_info[row['column']] = row['type']
                    except Exception as e:
                        logger.warning(f"Error fetching schema for {symbol}: {e}")

                    # 4. Daily Stats
                    try:
                        # Use SAMPLE BY 1d to get daily aggregation
                        query_daily = f"""
                            SELECT 
                                time as day, 
                                count() as count,
                                min(time) as min_time,
                                max(time) as max_time
                            FROM "{target_table}"
                            WHERE exchange = :exchange AND symbol = :symbol
                            SAMPLE BY 1d
                        """
                        daily_df = self._query_df(query_daily, {'exchange': exchange, 'symbol': symbol})
                        
                        if not daily_df.empty:
                            # Filter out days with 0 count if FILL(0) added them (though we want to see gaps, but maybe not pre-history)
                            # Actually, keeping 0 counts is good for visualizing gaps in the daily breakdown
                            
                            # Determine count column name again just in case
                            d_count_col = 'count' if 'count' in daily_df.columns else 'count()'
                            
                            for _, row in daily_df.iterrows():
                                # Only add if within the overall report range or if it has data
                                if row[d_count_col] > 0:
                                    report.daily_stats.append({
                                        'date': row['day'].date() if isinstance(row['day'], datetime) else row['day'],
                                        'count': int(row[d_count_col]),
                                        'min_time': row['min_time'],
                                        'max_time': row['max_time']
                                    })
                    except Exception as e:
                        logger.warning(f"Error calculating daily stats for {symbol}: {e}")

        except Exception as e:
            logger.error(f"Error analyzing tick quality for {symbol}: {e}")
            
        return report

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception as e:
                logger.error(f"Error closing ILP socket: {e}")
            finally:
                self._sock = None
        
        if self.engine:
            self.engine.dispose()
