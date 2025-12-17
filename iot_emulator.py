import json
import time
import random
import requests
import threading
from datetime import datetime

CONFIG_FILE = 'cfg.json'


def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)


def generate_payload(sensor_conf):
    val = random.uniform(sensor_conf['min_value'], sensor_conf['max_value'])

    if sensor_conf['type'] == 'light':
        val = int(val)
    else:
        val = round(val, 2)

    payload = {
        "sensor_id": sensor_conf['deviceId'],
        "timestamp": datetime.now().isoformat(),
        "type": sensor_conf['type'],
        "value": val,
        "unit": sensor_conf['unit'],
        "location": sensor_conf['location']
    }
    return payload


def run_sensor(sensor_conf, url):
    device_id = sensor_conf['deviceId']
    interval_sec = sensor_conf['interval_ms'] / 1000.0

    print(f"Started sensor: {device_id} ({sensor_conf['type']}) | Interval: {sensor_conf['interval_ms']}ms")

    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }

    while True:
        try:
            if random.random() < 0.1:
                broken_payload = {
                    "sensor_id": None,
                    "timestamp": "not-a-date",
                    "type": "unknown",
                    "value": "NaN"
                }

                response = requests.post(
                    url,
                    data=broken_payload,
                    headers=headers,
                    timeout=5
                )
                print(f"[{device_id}] >>> Sent BROKEN data (Test DLQ)")

            else:
                data = generate_payload(sensor_conf)
                response = requests.post(
                    url,
                    json=data,
                    headers=headers,
                    timeout=5
                )

            if response.status_code != 200:
                print(f"[{device_id}] Server responded: {response.status_code} - {response.text}")
            else:
                print(f"[{device_id}] Sent OK")

            time.sleep(interval_sec)

        except Exception as e:
            print(f"[{device_id}] Connection error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    try:
        config = load_config()
        api_key = config.get("x-api-key")
        target_url = config.get('queue_url')

        if not target_url:
            print("Error: 'queue_url' not found in config.")
            exit(1)

        sensors_list = config['sensors']

        print(f"Loaded config. Target URL: {target_url}")
        print(f"Found {len(sensors_list)} sensors.")
        print("NOTE: ~10% of messages will be corrupted to test DLQ logic.")

        threads = []
        for sensor in sensors_list:
            t = threading.Thread(target=run_sensor, args=(sensor, target_url))
            t.daemon = True
            t.start()
            threads.append(t)

        while True:
            time.sleep(1)

    except FileNotFoundError:
        print(f"Error: File '{CONFIG_FILE}' not found!")
    except KeyError as e:
        print(f"Error: Config file is missing key {e}")
    except KeyboardInterrupt:
        print("\nStopped by user.")