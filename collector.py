import asyncio
import websockets
import json
import os
from datetime import datetime

# Set parameters
ENVIRONMENT = "www"
MAX_FILE_SIZE_MB = 95  # GitHub has 100MB file size limit, keeping buffer

async def get_weekly_futures():
    msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/get_instruments",
        "params": {
            "currency": "BTC",
            "kind": "future"
        }
    }

    async with websockets.connect(f'wss://{ENVIRONMENT}.deribit.com/ws/api/v2') as websocket:
        await websocket.send(json.dumps(msg))
        response = await websocket.recv()
        instruments = json.loads(response)['result']
        
        # Filter for weekly futures
        weekly_instruments = [
            i['instrument_name'] 
            for i in instruments 
            if i['settlement_period'] == 'week'
        ]
        
        # Create channels list with order book subscription format
        channels = [f"book.{instrument}.none.20.100ms" for instrument in weekly_instruments]
        return channels

def get_new_filename():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    return os.path.join('data', f'updates_{timestamp}.json') # git rm *.json to remove al json files

async def save_orderbook_data(runtime_minutes=300):  # ~5 hours
    # First get the list of weekly futures channels
    channels = await get_weekly_futures()
    print(f"Subscribing to channels: {channels}")

    msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/subscribe",
        "params": {
            "channels": channels
        }
    }

    start_time = datetime.now()
    current_filename = get_new_filename()

    async with websockets.connect(f'wss://{ENVIRONMENT}.deribit.com/ws/api/v2') as websocket:
        await websocket.send(json.dumps(msg))
        
        try:
            while True:
                # Check runtime
                if (datetime.now() - start_time).total_seconds() > runtime_minutes * 60:
                    print("Runtime limit reached")
                    break

                response = await websocket.recv()
                
                # Check file size and rotate if needed
                if os.path.exists(current_filename) and os.path.getsize(current_filename) > MAX_FILE_SIZE_MB * 1024 * 1024:
                    current_filename = get_new_filename()

                # Append to file
                with open(current_filename, 'a') as f:
                    json.dump(json.loads(response), f)
                    f.write('\n')
                        
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")

def main():
    try:
        asyncio.run(save_orderbook_data())

    except KeyboardInterrupt:
        print("\nData collection stopped by user")

if __name__ == "__main__":
    main()