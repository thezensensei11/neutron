import logging
import socket
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from .storage import StorageBackend

logger = logging.getLogger(__name__)

class QuestDBStorage(StorageBackend):
    def __init__(self, host: str = 'localhost', ilp_port: int = 9009, pg_port: int = 8812, database: str = 'qdb', username: str = 'admin', password: str = 'quest'):
        self.host = host
        self.ilp_port = ilp_port
        self.pg_port = pg_port
        self.database = database
        self.username = username
        self.password = password
        
        # Initialize SQL Engine for querying
        # QuestDB uses PostgreSQL wire protocol
        url = f"postgresql://{username}:{password}@{host}:{pg_port}/{database}"
        self.engine = create_engine(url)
        
        # Initialize ILP socket (lazy connection or reconnect on send)
        self._sock = None

    def _connect_ilp(self):
        if self._sock:
            return
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self.host, self.ilp_port))
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB ILP: {e}")
            self._sock = None

    def _send_ilp(self, lines: List[str], batch_size: int = 5000):
        if not lines:
            return
        
        self._connect_ilp()
        if not self._sock:
            return

        total_lines = len(lines)
        for i in range(0, total_lines, batch_size):
            batch = lines[i:i + batch_size]
            try:
                # Join lines with newline
                payload = '\n'.join(batch) + '\n'
                self._sock.sendall(payload.encode('utf-8'))
            except Exception as e:
                logger.error(f"Error sending ILP data (batch {i//batch_size + 1}): {e}")
                # Try to reconnect once
                self._sock.close()
                self._sock = None
                try:
                    self._connect_ilp()
                    if self._sock:
                        self._sock.sendall(payload.encode('utf-8'))
                except Exception as retry_e:
                    logger.error(f"Retry failed: {retry_e}")
                    # If a batch fails, we might lose data. 
                    # Ideally we should raise or return False, but for now we log.
                    pass

    def _escape_tag(self, tag: str) -> str:
        return str(tag).replace(' ', r'\ ').replace(',', r'\,').replace('=', r'\=')

    def _escape_string(self, s: str) -> str:
        return f'"{str(s).replace("\"", "\\\"")}"'

    def _format_timestamp(self, dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1e9) # Nanoseconds

    def save_ohlcv(self, data: List[Dict[str, Any]]):
        if not data:
            return
        
        lines = []
        for row in data:
            # Measurement
            measurement = 'ohlcv'
            
            # Tags (indexed, low cardinality)
            tags = [
                f"exchange={self._escape_tag(row['exchange'])}",
                f"symbol={self._escape_tag(row['symbol'])}",
                f"timeframe={self._escape_tag(row['timeframe'])}",
                f"instrument_type={self._escape_tag(row.get('instrument_type', 'spot'))}"
            ]
            
            # Fields
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
        logger.info(f"Sent {len(lines)} OHLCV records to QuestDB")

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
        logger.info(f"Sent {len(lines)} tick records to QuestDB")

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
        logger.info(f"Sent {len(lines)} funding rates to QuestDB")

    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        if not data:
            return
            
        # Map generic types to measurements and fields
        # This is a simplified mapping, might need expansion
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
                elif isinstance(v, bool):
                    fields.append(f"{k}={'t' if v else 'f'}")
                elif isinstance(v, (int, float)):
                    fields.append(f"{k}={v}")
            
            if not fields:
                continue

            # Determine timestamp
            ts_val = row.get('time') or row.get('create_time') or row.get('open_time')
            if not ts_val:
                ts_val = datetime.now(timezone.utc)
            
            timestamp = self._format_timestamp(ts_val)
            
            line = f"{measurement},{','.join(tags)} {','.join(fields)} {timestamp}"
            lines.append(line)
            
        self._send_ilp(lines)
        logger.info(f"Sent {len(lines)} {data_type} records to QuestDB")

    def _query_df(self, query: str, params: Dict = None) -> pd.DataFrame:
        try:
            # Pre-process params to ensure QuestDB compatibility
            if params:
                new_params = {}
                for k, v in params.items():
                    if isinstance(v, datetime):
                        # Convert to naive UTC string for QuestDB
                        # QuestDB expects 'YYYY-MM-DDThh:mm:ss.mmmmmmZ' or similar
                        # We'll use ISO format but ensure it's simple
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
        df = self._query_df(query, params)
        if not df.empty:
            df.rename(columns={'timestamp': 'time'}, inplace=True)
        return df

    def load_generic_data(self, data_type: str, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        # Sanitize table name (data_type) to prevent injection, though internal use
        if not data_type.replace('_', '').isalnum():
            logger.warning(f"Invalid table name: {data_type}")
            return pd.DataFrame()
            
        query = f"""
            SELECT * FROM "{data_type}"
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

    def list_available_data(self, deep_scan: bool = False) -> List[Dict[str, Any]]:
        # This is tricky in QuestDB as we need to query multiple tables
        # We can query 'tables()' to get list of tables
        results = []
        
        try:
            tables_df = self._query_df("SHOW TABLES")
            if tables_df.empty:
                return results
                
            tables = tables_df['table_name'].tolist()
            
            for table in tables:
                # Skip system tables or unrelated
                if table in ['telemetry', 'telemetry_config']: continue
                
                # Check if table has standard columns (exchange, symbol)
                # We can try to query distinct exchange, symbol, timeframe
                try:
                    # QuestDB supports SAMPLE BY but for summary we just group by
                    query = f"""
                        SELECT 
                            exchange, 
                            symbol, 
                            first(instrument_type) as instrument_type,
                            first(timeframe) as timeframe,
                            min(timestamp) as start_date, 
                            max(timestamp) as end_date, 
                            count() as count
                        FROM "{table}"
                        GROUP BY exchange, symbol
                    """
                    # Note: timeframe might not exist in all tables, so this query might fail
                    # We need to inspect columns first or handle error
                    
                    # Better approach: Get columns first
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
                            min(timestamp) as start_date, 
                            max(timestamp) as end_date, 
                            count() as count
                        FROM "{table}"
                        GROUP BY exchange, symbol
                    """
                    
                    summary_df = self._query_df(query)
                    
                    for _, row in summary_df.iterrows():
                        results.append({
                            'data_type': table, # Table name acts as data type
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

    def close(self):
        """Close ILP socket and dispose SQL engine."""
        if self._sock:
            try:
                self._sock.close()
            except Exception as e:
                logger.error(f"Error closing ILP socket: {e}")
            finally:
                self._sock = None
        
        if self.engine:
            self.engine.dispose()
