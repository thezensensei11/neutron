import logging
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from ..core.storage import StorageBackend

logger = logging.getLogger(__name__)

class InfoService:
    def __init__(self, storage: StorageBackend):
        self.storage = storage

    def generate_summary(self, deep_scan: bool = False) -> str:
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
            report.append("| Symbol | Data Type | Timeframe | Start Date | End Date | Count | Continuity |")
            report.append("|---|---|---|---|---|---|---|")
            
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
                    
                # Continuity (placeholder)
                continuity = "✅" 
                if item.get('gaps'):
                    continuity = f"⚠️ {len(item['gaps'])} gaps"
                
                report.append(f"| {symbol} | {dtype} | {tf} | {start} | {end} | {count_str} | {continuity} |")
            
            report.append("")
            
        return "\n".join(report)
