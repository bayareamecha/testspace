import asyncio

import aiofiles

import csv

import signal

import logging

import tracemalloc

from datetime import datetime

from socrates import HOST, PORT, PROD_URL, POSITIONS, PORT_NAME

tracemalloc.start()

# Configuration
_POSITIONS_FILE = '_IP_Dict.csv'

# In-memory storage for readings
latest_sensor_data = {}  # {_position: latest_pressure}
last_pushed_readings = {}  # {_position: last_pushed_pressure}
last_pushed_readings_lock = asyncio.Lock()

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("async_server_error_log.txt"),
        logging.StreamHandler()
    ]
)

# Global variable to track the last logged time for each 
last_logged_time = {}
last_logged_time_lock = asyncio.Lock()

# Global variable to track the last push time for each 
last_push_time = {}
last_push_time_lock = asyncio.Lock()

# Global lock for accessing sensor data
sensor_data_lock = asyncio.Lock()

POSITIONS_ = {}
POSITIONS__lock = asyncio.Lock()

## Lucullus / Hermes modules
from amyris.client import LucullusClient
from amyris.util import attrdict

# Initialize Lucullus Client
client = LucullusClient(PROD_URL)

# Local  position to Controller IP map 
ip_to_ = {}

async def load__positions(csv_file):
    """Load  positions from a CSV file."""
    global ip_to_
    try:
        async with aiofiles.open(csv_file, mode='r') as file:
            content = await file.read()
            lines = content.splitlines()
            reader = csv.DictReader(lines)
            ip_to_ = {row['IPAddress']: row['Position'] for row in reader}
        logging.info(" positions loaded successfully.")
    except FileNotFoundError:
        logging.error(f"CSV file '{csv_file}' not found.")
    except Exception as e:
        logging.error(f"Error loading  positions: {e}")

async def parse_data(data):
    """Parse received data from a client."""
    parsed_data = []
    try:
        lines = data.split('\n')
        for line in lines:
            if line.strip() and line.startswith("Device IP:"):
                parts = line.split(', ')
                device_ip = parts[0].split(': ')[1].strip()
                pressure = float(parts[1].split(': ')[1].replace(' PSI', '').strip())
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                parsed_data.append([timestamp, device_ip, pressure])
    except Exception as e:
        logging.error(f"Error parsing data: {e}")
    return parsed_data

async def track_last_reading(parsed_data):
    global latest_sensor_data, last_pushed_readings

    try:
        for row in parsed_data:
            timestamp, device_ip, pressure, _position = row

        async with sensor_data_lock:
            # Update the latest reading for this  position
            latest_sensor_data[_position] = {
                "timestamp": timestamp,
                "device_ip": device_ip,
                "pressure": pressure,
                "_position": _position
            }

            logging.info(
                f"TRACK LAST READING -> : {_position}, Received Pressure: {pressure}, "
                f"Previous Stored Pressure: {latest_sensor_data.get(_position, {}).get('pressure')}"
            )

            # Retrieve the last pushed data for this  position
            last_pushed = last_pushed_readings.get(_position)

            # Extract the pressure from the last pushed data if it exists
            last_pushed_pressure = last_pushed.get("pressure") if last_pushed else None

            # 🛠 Debugging log: Check last pushed pressure
            logging.info(f": {_position}, Last Pushed Pressure: {last_pushed_pressure}, New Pressure: {pressure}")

            # Check if we need to push data
            if last_pushed_pressure is None or abs(pressure - last_pushed_pressure) >= 0.05:
                last_pushed_readings[_position] = {
                    "timestamp": timestamp,
                    "device_ip": device_ip,
                    "pressure": pressure,
                    "_position": _position
                }

                # Immediate push on change
                asyncio.create_task(push_data(_position, latest_sensor_data[_position]))


                # Log the push action
                logging.info(f"Updated last pushed data for  '{_position}': {pressure}")

            else:
                # 🛠 Debugging log: Check if the "else" is ever hit
                logging.info(f"DEBUG: No significant change for  '{_position}', Pressure: {pressure}, Last: {last_pushed_pressure}")
                logging.debug(f"No significant change for  '{_position}', not updating last pushed data.")
                
    except Exception as e:
        logging.error(f"Error tracking latest reading: {e}")
        
async def push_data(_position, latest_data):
    """Push data immediately when a change is detected."""
    if _position not in POSITIONS_:
        logging.warning(f" position {_position} not found in mappings.")
        return
    
    signal_id, _ = POSITIONS_[_position]
    latest_pressure = latest_data["pressure"]

    try:
        response = client.push_signal_data(signal_id, latest_pressure)
        if response.currentValue == latest_pressure:
            async with sensor_data_lock:
                last_pushed_readings[_position] = latest_data  # Ensure it's updating
                last_push_time[_position] = datetime.now()

            logging.info(f"Immediate push successful for '{_position}' -> {latest_pressure}")
        else:
            logging.error(f"Failed to push data for '{_position}', Response: {response}")

    except Exception as e:
        logging.error(f"Error pushing data for '{_position}': {e}")

async def handle_client(reader, writer):
    """Handle a single client connection."""
    try:
        addr = writer.get_extra_info('peername')
        logging.info(f"Connected by {addr}")

        while True:
            data = await reader.read(1024)
            if not data:
                break
            data_str = data.decode()

            # Parse the received data
            parsed_data = await parse_data(data_str)
            if parsed_data:
                for row in parsed_data:
                    device_ip = row[1]  # Extract device IP
                    pressure = row[2]  # Extract pressure reading
                    _position = ip_to_.get(device_ip, "Unknown")

                    if _position != "Unknown":
                        await track_last_reading([[row[0], device_ip, pressure, _position]])
                        logging.info(f"Updated latest reading for  '{_position}': {pressure}")
                    else:
                        logging.warning(f"Device IP '{device_ip}' not mapped to any  position.")

            writer.write(b"Data received.\n")
            await writer.drain()
    except Exception as e:
        logging.error(f"Error during communication: {e}")
    finally:
        writer.close()
        await writer.wait_closed()
        logging.info(f"Connection with {addr} closed.")

async def start_server():
    """Start the server."""
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addr = server.sockets[0].getsockname()
    logging.info(f"Serving on {addr}")

    async with server:
        await server.serve_forever()  # Brutal

async def push_signal_data_periodically():
    """Continuously push PSI signal data every X.X minutes or if it changes significantly."""
    global last_push_time, POSITIONS_, latest_sensor_data, last_pushed_readings


    while True:
        async with POSITIONS__lock:
            local_mappings = dict(POSITIONS_)  # Safely copy mappings

        for _position, (signal_id, _) in POSITIONS_.items():
            # Use the lock when accessing shared data
            async with sensor_data_lock:
                latest_data = latest_sensor_data.get(_position)
                latest_pressure = latest_data.get("pressure") if latest_data else None

            if latest_pressure is None:
                logging.debug(f"No latest reading for  '{_position}'. Skipping push.")
                continue

            last_pushed_data = last_pushed_readings.get(_position)
            last_pushed_pressure = last_pushed_data.get("pressure") if last_pushed_data else None
            last_time = last_push_time.get(_position)
            current_time = datetime.now()

            # Calculate push conditions
            push_due_to_pressure_change = (
                last_pushed_pressure is None or abs(latest_pressure - last_pushed_pressure) >= 0.05
            )
            
            # Debugging logs to confirm values
            logging.info(
                f"DEBUG -> : {_position}, Latest Pressure: {latest_pressure}, "
                f"Last Pushed Pressure: {last_pushed_pressure}, "
                f"Pressure Difference: {abs(latest_pressure - last_pushed_pressure) if last_pushed_pressure is not None else 'N/A'}, "
                f"Trigger Condition: {push_due_to_pressure_change}"
            )

            push_due_to_time = (
                last_time is None or (current_time - last_time).total_seconds() >= 900
            )

            # Debug logging to check why data might be pushed
            logging.debug(
                f": {_position}, Latest Pressure: {latest_pressure}, Last Pushed Pressure: {last_pushed_pressure}, "
                f"Pressure Change: {abs(latest_pressure - last_pushed_pressure) if last_pushed_pressure else 'N/A'}, "
                f"Time Since Last Push: {(current_time - last_time).total_seconds() if last_time else 'N/A'}, "
                f"Push Due to Pressure Change: {push_due_to_pressure_change}, Push Due to Time: {push_due_to_time}"
            )
            
            print("looping")
            # Decide whether to push based on the conditions
            if push_due_to_time:
                print('pushing')
                try:
                    # Push the data to Lucullus
                    response = client.push_signal_data(signal_id, latest_pressure)
                    print(response)
                    if response.currentValue == latest_pressure:
                        async with sensor_data_lock:
                            last_push_time[_position] = current_time

                        logging.info(f" Updated last pushed data for '{_position}' to {latest_pressure}")
                    else:
                        logging.error(f" Failed to push data for '{_position}', Response: {response}")

                except Exception as e:
                    logging.error(f"Error pushing data for '{_position}' -> Signal ID: {signal_id}: {e}")
                    continue  # Ensure the loop continues
                            # Decide whether to push based on the conditions

        # Short delay to avoid excessive looping
        await asyncio.sleep(5)  # Reduced sleep time to check every 300 seconds

async def update_POSITIONS_():

    """Refresh  mappings every 30 minutes."""
    global POSITIONS_
    updated_mappings = {}

    for _position in POSITIONS:
        try:
            # Fetch reactor details
            reactor = client.get_reactor_details(_position).data
            if not reactor:
                logging.warning(f"No reactor data found for  position '{_position}'")
                continue

            # Process ID
            process_id = reactor.get('process', {}).get('id')
            if not process_id:
                logging.warning(f"No process ID found for  position '{_position}'")
                continue

            # Signal ID for PORT_NAME
            signals = client.get_signals(process_id)
            signal = next((s for s in signals if s.port.name == PORT_NAME), None)
            if not signal:
                logging.warning(f"No signal found for port '{PORT_NAME}' in  position '{_position}'")
                continue

            signal_id = signal.id
            updated_mappings[_position] = (signal_id, process_id)

            logging.info(f"Mapping updated: {_position} -> Process ID: {process_id}, Signal ID: {signal_id}")

        except Exception as e:
            logging.error(f"Error updating mappings for  position '{_position}': {e}")
            continue

        # Safely update the global POSITIONS_
    async with POSITIONS__lock:
        POSITIONS_ = updated_mappings

    return updated_mappings

async def main():
    # Load  positions
    await load__positions(_POSITIONS_FILE)

    # Update  mappings initially
    global POSITIONS_
    POSITIONS_ = await update_POSITIONS_()


    # Start tasks
    server_task = asyncio.create_task(start_server())
    push_task = asyncio.create_task(push_signal_data_periodically())

    try:
        # Wait for tasks to complete
        await asyncio.gather(server_task, push_task)
    except Exception as e:
        logging.error(f"Exception in main loop: {e}")
    finally:
        logging.info("Shutting down...")

async def shutdown(signal, loop):
    """Handle shutdown signals."""
    logging.info(f"Received exit signal {signal.name}...")

    # Cancel all tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    # Allow tasks to finish
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()
    logging.info("Shutdown complete.")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s, loop)))
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Exception in main loop: {e}")