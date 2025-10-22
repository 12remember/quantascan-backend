#!/usr/bin/env python3
"""
Test script to run spider with limited blocks to avoid freezing
"""

import subprocess
import sys
import time

def run_spider_with_limited_blocks(start_block=0, end_block=50):
    """Run spider with a limited range of blocks to test performance."""
    
    print(f"🧪 Testing spider with blocks {start_block} to {end_block}")
    print("This will help identify if the freezing is due to too many concurrent requests.")
    
    # First, let's test with just a few blocks
    test_blocks = list(range(start_block, min(end_block, start_block + 10)))
    
    for block_num in test_blocks:
        print(f"\n📦 Processing block {block_num}...")
        
        try:
            # Run spider for a single block
            cmd = [
                "scrapy", "crawl", "qrl_network_spider", 
                "-a", f"block={block_num}",
                "-L", "INFO"
            ]
            
            print(f"Running: {' '.join(cmd)}")
            
            # Run with timeout
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=120  # 2 minute timeout per block
            )
            
            if result.returncode == 0:
                print(f"✅ Block {block_num} completed successfully")
                print(f"Output: {result.stdout[-200:]}...")  # Last 200 chars
            else:
                print(f"❌ Block {block_num} failed")
                print(f"Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Block {block_num} timed out after 2 minutes")
        except Exception as e:
            print(f"❌ Error processing block {block_num}: {e}")
        
        # Add delay between blocks
        time.sleep(2)
    
    print(f"\n🎯 Test completed for blocks {start_block} to {min(end_block, start_block + 10)}")
    print("If this works well, you can increase the range or run the full spider.")

def run_spider_with_retry():
    """Run spider with retry mode to handle failed transactions."""
    
    print("🔄 Running spider in retry mode to handle failed transactions...")
    
    try:
        cmd = [
            "scrapy", "crawl", "qrl_network_spider", 
            "-a", "retry=transactions",
            "-L", "INFO"
        ]
        
        print(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ Retry mode completed successfully")
            print(f"Output: {result.stdout[-500:]}...")
        else:
            print("❌ Retry mode failed")
            print(f"Error: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print("⏰ Retry mode timed out after 5 minutes")
    except Exception as e:
        print(f"❌ Error in retry mode: {e}")

if __name__ == "__main__":
    print("🔧 QRL Spider Test Script")
    print("=" * 50)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "retry":
            run_spider_with_retry()
        elif sys.argv[1] == "test":
            start_block = int(sys.argv[2]) if len(sys.argv) > 2 else 0
            end_block = int(sys.argv[3]) if len(sys.argv) > 3 else 50
            run_spider_with_limited_blocks(start_block, end_block)
        else:
            print("Usage:")
            print("  python test_spider_limited.py test [start_block] [end_block]  # Test with limited blocks")
            print("  python test_spider_limited.py retry                          # Run retry mode")
    else:
        print("Usage:")
        print("  python test_spider_limited.py test [start_block] [end_block]  # Test with limited blocks")
        print("  python test_spider_limited.py retry                          # Run retry mode")
        print("\nExample:")
        print("  python test_spider_limited.py test 3752100 3752120")
        print("  python test_spider_limited.py retry") 