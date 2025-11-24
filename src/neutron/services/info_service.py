import logging
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from ..core.storage import StorageBackend, DataQualityReport

logger = logging.getLogger(__name__)

class InfoService:
    def __init__(self, storages: List[StorageBackend]):
        self.storages = storages

    def generate_summary(self, deep_scan: bool = True, show_gaps: bool = True) -> str:
        """
        Generate a human-readable summary of available data with deep quality analysis.
        """
        # 1. Discovery Phase
        inventory = []
        for storage in self.storages:
            try:
                # Fast scan to find what we have
                data = storage.list_available_data(deep_scan=False)
                for item in data:
                    item['storage'] = storage # Keep track of which storage has this
                inventory.extend(data)
            except Exception as e:
                logger.error(f"Failed to list available data from storage {storage}: {e}")
        
        if not inventory:
            return "No data found in storage."
            
        # 2. Analysis Phase
        analyzed_data = []
        for item in inventory:
            storage = item['storage']
            exchange = item['exchange']
            symbol = item['symbol']
            instrument_type = item['instrument_type']
            data_type = item['data_type']
            timeframe = item.get('timeframe')
            
            try:
                report = None
                if data_type == 'ohlcv':
                    report = storage.analyze_ohlcv_quality(exchange, symbol, timeframe, instrument_type)
                elif data_type in ['tick', 'aggTrades', 'trades']:
                    report = storage.analyze_tick_quality(exchange, symbol, instrument_type, data_type)
                
                if report:
                    analyzed_data.append(report)
                else:
                    # Fallback if analysis not supported or failed silently
                    analyzed_data.append(DataQualityReport(
                        exchange=exchange, symbol=symbol, instrument_type=instrument_type, 
                        data_type=data_type, timeframe=timeframe,
                        start_date=item['start_date'], end_date=item['end_date'],
                        total_rows=item['count']
                    ))
            except Exception as e:
                logger.error(f"Analysis failed for {symbol} {data_type}: {e}")
                # Add basic info
                analyzed_data.append(DataQualityReport(
                    exchange=exchange, symbol=symbol, instrument_type=instrument_type, 
                    data_type=data_type, timeframe=timeframe,
                    start_date=item['start_date'], end_date=item['end_date'],
                    total_rows=item['count']
                ))

        # 3. Reporting Phase
        # Group by Exchange -> Instrument
        grouped = {}
        for report in analyzed_data:
            key = (report.exchange, report.instrument_type)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(report)
            
        report_lines = ["# Neutron Data Storage Summary", ""]
        report_lines.append(f"**Generated at:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"**Deep Scan:** {'Enabled' if deep_scan else 'Disabled'}")
        report_lines.append("")
        
        for (exchange, instrument), items in grouped.items():
            report_lines.append(f"## {exchange.upper()} ({instrument})")
            
            # Sort items by data type then symbol
            items.sort(key=lambda x: (x.data_type, x.symbol))
            
            # Create table
            report_lines.append("| Symbol | Data Type | Timeframe | Start Date | End Date | Count | Quality | Volume | Price Range | Trades | Continuity |")
            report_lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
            
            gap_details = []
            analytics_details = []
            
            for item in items:
                # Format Count
                count = item.total_rows
                if count > 1_000_000:
                    count_str = f"{count/1_000_000:.1f}M"
                elif count > 1_000:
                    count_str = f"{count/1_000:.1f}K"
                else:
                    count_str = str(count)
                    
                # Format Dates
                start_str = item.start_date.strftime('%Y-%m-%d') if item.start_date else "-"
                end_str = item.end_date.strftime('%Y-%m-%d') if item.end_date else "-"
                
                # Quality Metrics
                quality_str = f"{item.quality_score:.1f}%" if item.quality_score > 0 else "-"
                
                # Rich Analytics Display
                vol_str = "-"
                if item.volume_stats:
                    vol = item.volume_stats.get('total', 0)
                    if vol > 1_000_000: vol_str = f"{vol/1_000_000:.1f}M"
                    elif vol > 1_000: vol_str = f"{vol/1_000:.1f}K"
                    else: vol_str = f"{vol:.1f}"
                    
                price_str = "-"
                if item.price_stats:
                    min_p = item.price_stats.get('min', 0)
                    max_p = item.price_stats.get('max', 0)
                    price_str = f"{min_p:.2f}-{max_p:.2f}"
                    
                trades_str = "-"
                if item.trade_stats:
                    t_count = item.trade_stats.get('count', 0)
                    if t_count > 1_000_000: trades_str = f"{t_count/1_000_000:.1f}M"
                    elif t_count > 1_000: trades_str = f"{t_count/1_000:.1f}K"
                    else: trades_str = str(t_count)
                
                # Continuity
                gap_count = len(item.gaps)
                if gap_count > 0:
                    total_gap_dur = sum([g[2].total_seconds() for g in item.gaps])
                    
                    def fmt_dur(seconds):
                        if seconds > 86400: return f"{seconds/86400:.1f}d"
                        if seconds > 3600: return f"{seconds/3600:.1f}h"
                        return f"{seconds/60:.1f}m"
                        
                    continuity = f"⚠️ {gap_count} gaps ({fmt_dur(total_gap_dur)})"
                    
                    if show_gaps:
                        for start, end, dur in item.gaps:
                            gap_details.append(f"- **{item.symbol}** ({item.data_type}): {start} -> {end} (Duration: {dur})")
                else:
                    continuity = "✅ Continuous"
                
                tf_display = item.timeframe or '-'
                
                report_lines.append(f"| {item.symbol} | {item.data_type} | {tf_display} | {start_str} | {end_str} | {count_str} | {quality_str} | {vol_str} | {price_str} | {trades_str} | {continuity} |")
                
                # Collect detailed analytics for section below
                if item.trade_stats.get('buy_ratio'):
                    buy_pct = item.trade_stats['buy_ratio'] * 100
                    sell_pct = item.trade_stats['sell_ratio'] * 100
                    analytics_details.append(f"- **{item.symbol}** ({item.data_type}): Buy {buy_pct:.1f}% / Sell {sell_pct:.1f}%")

            if analytics_details:
                report_lines.append("")
                report_lines.append("### Market Dynamics (Buy/Sell Ratio)")
                report_lines.extend(analytics_details)

            # Schema Information
            schema_lines = []
            for item in items:
                if item.schema_info:
                    schema_str = ", ".join([f"{k} ({v})" for k, v in item.schema_info.items()])
                    schema_lines.append(f"- **{item.symbol}** ({item.data_type}): {schema_str}")
            
            if schema_lines:
                report_lines.append("")
                report_lines.append("### Schema Information")
                report_lines.extend(schema_lines)

            # Daily Breakdown
            daily_lines = []
            for item in items:
                if item.daily_stats:
                    daily_lines.append(f"#### {item.symbol} ({item.data_type}) Daily Counts")
                    # Sort by date
                    sorted_stats = sorted(item.daily_stats, key=lambda x: x['date'])
                    
                    # Create a mini-table for daily stats
                    daily_lines.append("| Date | Count | Min Time | Max Time |")
                    daily_lines.append("|---|---|---|---|")
                    
                    for stat in sorted_stats:
                        d_str = stat['date'].strftime('%Y-%m-%d') if isinstance(stat['date'], (datetime, pd.Timestamp)) else str(stat['date'])
                        c_str = f"{stat['count']:,}"
                        min_t = stat.get('min_time', '-')
                        max_t = stat.get('max_time', '-')
                        
                        # Format timestamps if present
                        if isinstance(min_t, datetime): min_t = min_t.strftime('%H:%M:%S')
                        if isinstance(max_t, datetime): max_t = max_t.strftime('%H:%M:%S')
                        
                        daily_lines.append(f"| {d_str} | {c_str} | {min_t} | {max_t} |")
                    daily_lines.append("")

            if daily_lines:
                report_lines.append("")
                report_lines.append("### Daily Breakdown")
                report_lines.extend(daily_lines)

            if gap_details:
                report_lines.append("")
                report_lines.append("### Gap Details")
                report_lines.extend(gap_details)
            
            report_lines.append("")
            
        return "\n".join(report_lines)

    def get_gap_report(self, deep_scan: bool = True) -> List[Dict[str, Any]]:
        """
        Generate a structured report of all gaps found in storage.
        Returns a list of dicts defining the gaps to be filled.
        """
        all_data = []
        for storage in self.storages:
            try:
                data = storage.list_available_data(deep_scan=deep_scan)
                all_data.extend(data)
            except Exception as e:
                logger.error(f"Failed to list available data for gap report from {storage}: {e}")

        gap_tasks = []
        
        for item in all_data:
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
