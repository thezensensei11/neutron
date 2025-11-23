import logging
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.crawler import DataCrawler

logging.basicConfig(level=logging.INFO)

def main():
    print("Initializing DataCrawler...")
    # Use database storage as populated by reproduce_notebook.py
    crawler = DataCrawler(storage_type='database')
    
    print("Getting InfoService...")
    info_service = crawler.get_info_service()
    
    print("Generating Summary (Deep Scan)...")
    #summary = info_service.generate_summary(deep_scan=False)
    summary = info_service.generate_summary(deep_scan=True, show_gaps=False)
    
    print("\n" + "="*50)
    print(summary)
    print("="*50 + "\n")

    # Optional: Test deep scan if needed, but fast scan is enough to verify structure
    # print("Generating Summary (Deep Scan)...")
    # summary_deep = info_service.generate_summary(deep_scan=True)
    # print(summary_deep)

if __name__ == "__main__":
    main()
