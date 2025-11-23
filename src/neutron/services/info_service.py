import logging
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from ..core.storage import StorageBackend

logger = logging.getLogger(__name__)

class InfoService:
    def __init__(self, storage: StorageBackend):
        self.storage = storage

    def generate_summary(self, deep_scan: bool = False, show_gaps: bool = False) -> str:
        """
        Generate a human-readable summary of available data.
        """
        try:
            data = self.storage.list_available_data(deep_scan=deep_scan)
        except Exception as e:
            logger.error(f"Failed to list available data: {e}")
            return f"Error generating summary: {e}"
        
        if not data:
            return "No data found in storage."
            
        # Group data by Exchange -> Instrument
        grouped = {}
        for item in data:
            exchange = item.get('exchange', 'Unknown')
            instrument = item.get('instrument_type', 'Unknown')
            key = (exchange, instrument)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(item)
            
        # Build report
        report = ["# Neutron Data Storage Summary", ""]
        report.append(f"**Generated at:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"**Deep Scan:** {'Enabled' if deep_scan else 'Disabled'}")
        report.append("")
        
        for (exchange, instrument), items in grouped.items():
            report.append(f"## {exchange.upper()} ({instrument})")
            
            # Sort items by data type then symbol
            items.sort(key=lambda x: (x['data_type'], x['symbol']))
            
            # Create table
            report.append("| Symbol | Data Type | Timeframe | Start Date | End Date | Count | Quality | Continuity |")
            report.append("|---|---|---|---|---|---|---|---|")
            
            gap_details = []
            
            for item in items:
                symbol = item['symbol']
                dtype = item['data_type']
                tf = item.get('timeframe') or '-'
                start = item['start_date']
                end = item['end_date']
                count = item['count']
                unit = item.get('count_unit', 'rows')
                
                # Format count
                if count > 1_000_000:
                    count_str = f"{count/1_000_000:.1f}M {unit}"
                elif count > 1_000:
                    count_str = f"{count/1_000:.1f}K {unit}"
                else:
                    count_str = f"{count} {unit}"
                    
                # Calculate Quality Score
                quality_str = "-"
                if tf and tf != '-' and start and end:
                    try:
                        # Parse timeframe to seconds
                        tf_map = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}
                        tf_seconds = tf_map.get(tf)
                        if tf_seconds:
                            total_seconds = (end - start).total_seconds()
                            expected_count = total_seconds / tf_seconds
                            if expected_count > 0:
                                quality = (count / expected_count) * 100
                                quality = min(quality, 100.0) # Cap at 100%
                                quality_str = f"{quality:.1f}%"
                            
                            interpolated_count = item.get('interpolated_count', 0)
                            interpolated_pct = (interpolated_count / count * 100) if count > 0 else 0.0
                            interpolated_pct_str = f"{interpolated_pct:.1f}%"
                    except:
                        pass

                # Continuity & Gap Metrics
                continuity = "✅" 
                if item.get('gaps'):
                    gap_count = len(item['gaps'])
                    gaps = item['gaps']
                    
                    # Metrics
                    total_gap_duration = sum([(g_end - g_start).total_seconds() for g_start, g_end in gaps])
                    max_gap = max([(g_end - g_start).total_seconds() for g_start, g_end in gaps])
                    avg_gap = total_gap_duration / gap_count if gap_count > 0 else 0
                    
                    # Format durations helper
                    def fmt_dur(seconds):
                        if seconds > 86400: return f"{seconds/86400:.1f}d"
                        if seconds > 3600: return f"{seconds/3600:.1f}h"
                        return f"{seconds/60:.1f}m"

                    if gap_count > 0:
                        continuity = f"⚠️ {gap_count} gaps (Total: {fmt_dur(total_gap_duration)}, Max: {fmt_dur(max_gap)})"
                    else:
                        continuity = "✅ Continuous"
                    
                    # Collect gap details if requested
                    if show_gaps:
                        for g_start, g_end in gaps:
                            g_dur = g_end - g_start
                            gap_details.append(f"- **{symbol}** ({dtype}): {g_start} -> {g_end} (Duration: {g_dur})")
                
                report.append(f"| {symbol} | {dtype} | {tf} | {start} | {end} | {count_str} | {quality_str} | {interpolated_pct_str} | {continuity} |")
            
            if gap_details:
                report.append("")
                report.append("### Gap Details")
                report.extend(gap_details)
            
            report.append("")
            
        return "\n".join(report)

    def get_gap_report(self, deep_scan: bool = True) -> List[Dict[str, Any]]:
        """
        Generate a structured report of all gaps found in storage.
        Returns a list of dicts defining the gaps to be filled.
        """
        try:
            data = self.storage.list_available_data(deep_scan=deep_scan)
        except Exception as e:
            logger.error(f"Failed to list available data for gap report: {e}")
            return []
            
        gap_tasks = []
        
        for item in data:
            gaps = item.get('gaps', [])
            if not gaps:
                continue
                
            exchange = item.get('exchange')
            symbol = item.get('symbol')
            instrument_type = item.get('instrument_type', 'spot')
            timeframe = item.get('timeframe')
            data_type = item.get('data_type')
            
            # Only support OHLCV for now as backfill service is specialized for it
            if data_type != 'ohlcv':
                continue
                
            for start, end in gaps:
                gap_tasks.append({
                    'exchange': exchange,
                    'symbol': symbol,
                    'instrument_type': instrument_type,
                    'timeframe': timeframe,
                    'start_date': start,
                    'end_date': end,
                    'duration': (end - start).total_seconds()
                })
                
        return gap_tasks
