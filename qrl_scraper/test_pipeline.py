import os
import sys
import environ
from datetime import datetime, timezone

# Add the current directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from qrlNetwork.items import QRLNetworkTransactionItem
from qrlNetwork.pipelines import QrlnetworkPipeline_transaction

def test_pipeline():
    """Test the transaction pipeline with sample data."""
    
    print("=== Testing Transaction Pipeline ===")
    
    # Create a sample transaction item (similar to what the spider would create)
    item = QRLNetworkTransactionItem()
    
    # Fill in the required fields
    item['transaction_hash'] = 'TEST_PIPELINE_HASH_123'
    item['transaction_sending_wallet_address'] = 'Q0105008e06cb4c3b33bc7c7f22a53f6620a4ff2d9b23d7274947a80d08bf99314281337dfc0f93'
    item['transaction_receiving_wallet_address'] = 'Q010500b8601fb018af63f22b31854f649f32249ffd7c2e887d80694b458bd18ee6ca9f9806c016'
    item['transaction_amount_send'] = 1000000000
    item['transaction_type'] = 'transfer'
    item['transaction_block_number'] = 999999
    item['transaction_found'] = True
    item['transaction_result'] = 'transaction'
    item['spider_name'] = 'test_spider'
    item['spider_version'] = '1.0'
    item['master_addr_type'] = 'Buffer'
    item['master_addr_data'] = '0105008e06cb4c3b33bc7c7f22a53f6620a4ff2d9b23d7274947a80d08bf99314281337dfc0f93'
    item['master_addr_fee'] = 0  # This was the missing field!
    item['public_key_type'] = 'Buffer'
    item['public_key_data'] = 'TEST_PUBLIC_KEY'
    item['signature_type'] = 'Buffer'
    item['transaction_nonce'] = 1
    item['transaction_addrs_to_type'] = 'Buffer'
    item['block_found_datetime'] = int(datetime.now(timezone.utc).timestamp())
    item['item_url'] = 'https://test.example.com/tx/TEST_PIPELINE_HASH_123'
    
    print(f"Created test item with hash: {item['transaction_hash']}")
    
    # Create a mock spider object
    class MockSpider:
        def __init__(self):
            self.name = 'test_spider'
            self.logger = type('MockLogger', (), {'info': print, 'error': print})()
    
    spider = MockSpider()
    
    # Test the pipeline
    pipeline = QrlnetworkPipeline_transaction()
    
    try:
        print("Processing item through pipeline...")
        result = pipeline.process_item(item, spider)
        print("✅ Pipeline processing completed successfully!")
        print(f"Result item type: {type(result)}")
        
        # Check if the item was processed
        if result == item:
            print("✅ Item was returned by pipeline (not dropped)")
        else:
            print("⚠️ Item was modified or dropped by pipeline")
            
    except Exception as e:
        print(f"❌ Pipeline processing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pipeline() 