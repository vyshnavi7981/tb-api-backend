# main_simulator.py

import os
import time
import random
import requests
import concurrent.futures
from device_parser import parse_device_config

# ThingsBoard host and simulation timing configuration
TB_HOST = os.getenv("TB_HOST", "https://thingsboard.cloud")
POST_TIMEOUT = float(os.getenv("TB_POST_TIMEOUT", "10"))
TICK_SECONDS = float(os.getenv("SIM_TICK_SECONDS", "1.0"))

# Sensor baseline ranges for simulated values
VIBE_BASE = (0.02, 0.15)
JERK_BASE = (0.02, 0.15)
TEMP_RANGE = (28.0, 36.0)
HUMID_RANGE = (40.0, 65.0)
MIC_RANGE = (30.0, 55.0)
# main_simulator.py

import os
import time
import random
import requests
import concurrent.futures
from device_parser import parse_device_config

TB_HOST = os.getenv("TB_HOST", "https://thingsboard.cloud")
POST_TIMEOUT = float(os.getenv("TB_POST_TIMEOUT", "10"))
TICK_SECONDS = float(os.getenv("SIM_TICK_SECONDS", "1.0"))

VIBE_BASE = (0.02, 0.15)
JERK_BASE = (0.02, 0.15)
TEMP_RANGE = (28.0, 36.0)
HUMID_RANGE = (40.0, 65.0)
MIC_RANGE = (30.0, 55.0)


def choose_csv_file(folder=".", extension=".csv"):
    """
    Prompt user to select a CSV file containing device configuration.
    Returns the filename selected by the user.
    """
    csv_files = [f for f in os.listdir(folder) if f.endswith(extension)]
    if not csv_files:
        print("No CSV files found.")
        exit(1)
    print("Available Device CSVs:\n")
    for idx, file in enumerate(csv_files):
        print(f"{idx+1}. {file}")
    choice = input("\nEnter the number of the CSV to use: ").strip()
    try:
        index = int(choice) - 1
        if 0 <= index < len(csv_files):
            return csv_files[index]
        else:
            raise ValueError
    except ValueError:
        print("Invalid choice.")
        exit(1)


def tb_url_for_token(token: str) -> str:
    """
    Construct ThingsBoard telemetry API URL for a given device token.
    """
    return f"{TB_HOST}/api/v1/{token}/telemetry"


def send_json(url: str, payload: dict):
    """
    Send a JSON payload to the specified ThingsBoard URL.
    Prints the response status and a snippet of the response text.
    """
    print("Sending payload:", payload)
    try:
        r = requests.post(url, headers={"Content-Type": "application/json"}, json=payload, timeout=POST_TIMEOUT)
        print("Response:", r.status_code, r.text[:200])
    except requests.RequestException as e:
        print("Failed to send:", e)


def height_to_laser_val(height_mm: float, max_boundary: float) -> float:
    """
    Convert a lift height in mm to the simulated laser sensor value.
    Ensures the value is non-negative and rounded to 2 decimals.
    """
    lv = max_boundary - height_mm
    if lv < 0:
        lv = 0.0
    return round(lv, 2)


def random_noise(lo_hi):
    """
    Generate a random float within the given range, rounded to 4 decimals.
    Used for simulating sensor noise.
    """
    lo, hi = lo_hi
    return round(random.uniform(lo, hi), 4)


def base_sensor_payload():
    """
    Generate a dictionary of simulated sensor readings for a lift device.
    """
    return {
        "accel_x_val": random_noise(VIBE_BASE),
        "accel_y_val": random_noise(VIBE_BASE),
        "accel_z_val": random_noise(VIBE_BASE),
        "gyro_x_val":  random_noise(JERK_BASE),
        "gyro_y_val":  random_noise(JERK_BASE),
        "gyro_z_val":  random_noise(JERK_BASE),
        "mpu_temp_val": round(random.uniform(*TEMP_RANGE), 2),
        "humidity_val": round(random.uniform(*HUMID_RANGE), 2),
        "mic_val": round(random.uniform(*MIC_RANGE), 2)
    }


def run_alarm_tester():
    """
    Interactive mode to trigger single-shot alarm scenarios for a selected device.
    Prompts user for device and alarm type, then sends corresponding telemetry.
    """
    print("\nAvailable Devices from selected CSV:")
    # List all devices parsed from the selected CSV file
    # Prompt user to select an alarm scenario to trigger
    def gen_laser_for_height(h):
        # Helper to convert height to laser value for this device
        return height_to_laser_val(h, max_boundary)

    def generate_nearby_height(base=4000, tolerance=50):
        # Helper to generate a height near a base value, simulating lift position
        return round(random.uniform(base - tolerance, base + tolerance), 1)

    # Each branch below simulates a different alarm scenario by sending telemetry
    selected_csv = choose_csv_file()
    devices = parse_device_config(selected_csv)

    print("\nAvailable Devices from selected CSV:")
    for i, device in enumerate(devices):
        boundaries = device['floor_boundaries']
        print(f"{i+1}. Token: {device['token']} | Floors: {len(boundaries)} | Range: {boundaries[0]} - {boundaries[-1]} mm")

    try:
        device_index = int(input("Select a device number to test: ")) - 1
        selected_device = devices[device_index]
        access_token = selected_device['token']
    except (ValueError, IndexError):
        print("Invalid selection.")
        return

    url = tb_url_for_token(access_token)
    max_boundary = selected_device['floor_boundaries'][-1]

    print("\nSelect Alarm to Trigger:")
    print("1 - Vibration X/Y (single event)")
    print("2 - Vibration Z (single event)")
    print("3 - Jerks X/Y (single event)")
    print("4 - Jerks Z (single event)")
    print("5 - Humidity high")
    print("6 - Temperature high")
    print("7 - Door open too long (>15s)")
    print("8 - Idle for 5 minutes at fixed height")
    print("9 - Door mismatch (OPEN away from floor boundary)")
    print("10 - Bucket-Based XYZ Vibration/Jerk Alarm (3 hits in zone)")
    print("11 - Idle Test (Outside Home Floor, 3 min)")
    print("12 - Idle Test (At Home Floor, 3 min)")
    print("13 - Door Open/Close Count Simulation")
    print("14 - Door Open Duration Simulation")
    print("15 - Combined Multi-Floor Door Cycles")

    choice = input("Enter a number (1-15): ").strip()

    def gen_laser_for_height(h):
        return height_to_laser_val(h, max_boundary)

    def generate_nearby_height(base=4000, tolerance=50):
        return round(random.uniform(base - tolerance, base + tolerance), 1)

    if choice == "1":
        # High accel X/Y
        height = generate_nearby_height()
        payload = base_sensor_payload()
        payload.update({
            "accel_x_val": 5.5,
            "accel_y_val": 5.1,
            "laser_val": gen_laser_for_height(height),
            "door_val": "CLOSE"
        })
        send_json(url, payload)

    elif choice == "2":
        height = generate_nearby_height()
        payload = base_sensor_payload()
        payload.update({
            "accel_z_val": 15.5,
            "laser_val": gen_laser_for_height(height),
            "door_val": "CLOSE"
        })
        send_json(url, payload)

    elif choice == "3":
        height = generate_nearby_height()
        payload = base_sensor_payload()
        payload.update({
            "gyro_x_val": 5.6,
            "gyro_y_val": 5.7,
            "laser_val": gen_laser_for_height(height),
            "door_val": "CLOSE"
        })
        send_json(url, payload)

    elif choice == "4":
        height = generate_nearby_height()
        payload = base_sensor_payload()
        payload.update({
            "gyro_z_val": 15.8,
            "laser_val": gen_laser_for_height(height),
            "door_val": "CLOSE"
        })
        send_json(url, payload)

    elif choice == "5":
        payload = base_sensor_payload()
        payload.update({
            "humidity_val": 70,
            "laser_val": gen_laser_for_height(4000),
            "door_val": "CLOSE"
        })
        send_json(url, payload)

    elif choice == "6":
        payload = base_sensor_payload()
        payload.update({
            "mpu_temp_val": 60,
            "laser_val": gen_laser_for_height(4000),
            "door_val": "CLOSE"
        })
        send_json(url, payload)

    elif choice == "7":
        print("Simulating door open >15s for timer-based alarm")
        open_payload = base_sensor_payload()
        open_payload.update({
            "door_val": "OPEN",
            "laser_val": gen_laser_for_height(4000)
        })
        send_json(url, open_payload)
        for _ in range(16):
            tick_payload = base_sensor_payload()
            tick_payload.update({"laser_val": gen_laser_for_height(4000)})
            send_json(url, tick_payload)
            time.sleep(1)
        close_payload = base_sensor_payload()
        close_payload.update({
            "door_val": "CLOSE",
            "laser_val": gen_laser_for_height(4000)
        })
        send_json(url, close_payload)

    elif choice == "8":
        print("Simulating idle lift at fixed height for 5 minutes...")
        for _ in range(300):
            payload = base_sensor_payload()
            payload.update({"laser_val": gen_laser_for_height(4000), "door_val": "CLOSE"})
            send_json(url, payload)
            time.sleep(1)

    elif choice == "9":
        print("Triggering DOOR MISMATCH...")
        boundaries = selected_device['floor_boundaries']
        floor = random.choice(boundaries)
        offset = random.choice([-1, 1]) * random.randint(11, 50)
        height = floor + offset
        payload = base_sensor_payload()
        payload.update({"door_val": "OPEN", "laser_val": gen_laser_for_height(height)})
        send_json(url, payload)

    elif choice == "10":
        print("Triggering Bucket-Based XYZ Vibration/Jerk Alarm")
        key = random.choice(["gyro_z_val", "accel_z_val", "gyro_x_val", "gyro_y_val", "accel_x_val", "accel_y_val"])
        base_height = generate_nearby_height(4000, tolerance=10)
        bucket_range = 50
        values = {
            "gyro_x_val": 5.6, "gyro_y_val": 5.6, "gyro_z_val": 15.6,
            "accel_x_val": 5.6, "accel_y_val": 5.6, "accel_z_val": 15.6
        }
        for _ in range(3):
            height = round(random.uniform(base_height - bucket_range + 1, base_height + bucket_range - 1), 1)
            payload = base_sensor_payload()
            payload.update({key: values[key], "laser_val": gen_laser_for_height(height), "door_val": "CLOSE"})
            send_json(url, payload)
            time.sleep(1)

    elif choice == "11":
        print("Simulating idle outside home floor for 3 minutes...")
        home_floor_index = selected_device.get('home_floor_index', 1)
        outside_index = (home_floor_index + 1) % len(selected_device['floor_boundaries'])
        height = selected_device['floor_boundaries'][outside_index]
        for _ in range(180):
            payload = base_sensor_payload()
            payload.update({"laser_val": gen_laser_for_height(height), "door_val": "CLOSE"})
            send_json(url, payload)
            time.sleep(1)

    elif choice == "12":
        print("Simulating idle at home floor for 3 minutes...")
        home_floor_index = selected_device.get('home_floor_index', 1)
        height = selected_device['floor_boundaries'][home_floor_index]
        for _ in range(180):
            payload = base_sensor_payload()
            payload.update({"laser_val": gen_laser_for_height(height), "door_val": "CLOSE"})
            send_json(url, payload)
            time.sleep(1)

    elif choice == "13":
        print("Simulating door open/close count on each floor...")
        for floor_index, height in enumerate(selected_device['floor_boundaries']):
            print(f"Testing floor {floor_index}...")
            for _ in range(5):
                open_payload = base_sensor_payload()
                open_payload.update({"door_val": "OPEN", "laser_val": gen_laser_for_height(height)})
                send_json(url, open_payload)
                time.sleep(2)
                close_payload = base_sensor_payload()
                close_payload.update({"door_val": "CLOSE", "laser_val": gen_laser_for_height(height)})
                send_json(url, close_payload)
                time.sleep(1)
        print("Completed door open/close count simulation.")

    elif choice == "14":
        print("Simulating prolonged door open for duration tracking...")
        for floor_index, height in enumerate(selected_device['floor_boundaries']):
            print(f"Floor {floor_index}: door open 10s")
            open_payload = base_sensor_payload()
            open_payload.update({"door_val": "OPEN", "laser_val": gen_laser_for_height(height)})
            send_json(url, open_payload)
            time.sleep(10)
            close_payload = base_sensor_payload()
            close_payload.update({"door_val": "CLOSE", "laser_val": gen_laser_for_height(height)})
            send_json(url, close_payload)
            time.sleep(2)
        print("Completed door open duration simulation.")

    elif choice == "15":
        print("Running combined multi-floor door open/close simulation...")
        for cycle in range(3):
            for floor_index, height in enumerate(selected_device['floor_boundaries']):
                print(f"Cycle {cycle+1}, Floor {floor_index}")
                open_payload = base_sensor_payload()
                open_payload.update({"door_val": "OPEN", "laser_val": gen_laser_for_height(height)})
                send_json(url, open_payload)
                time.sleep(5)
                close_payload = base_sensor_payload()
                close_payload.update({"door_val": "CLOSE", "laser_val": gen_laser_for_height(height)})
                send_json(url, close_payload)
                time.sleep(2)
        print("Completed combined multi-floor simulation.")

    else:
        print("Invalid choice.")


def run_full_simulator():
    """
    Main simulation loop for all devices.
    Simulates lift movement, door open/close cycles, and sends periodic telemetry.
    Each device acts as a state machine: MOVING or DOOR_OPEN.
    """
    # Initialize device state for simulation
    selected_csv = choose_csv_file()
    print(f"\nStarting simulator using: {selected_csv}")
    devices = parse_device_config(selected_csv)

    for d in devices:
        d.setdefault("current_height_mm", d['floor_boundaries'][0])
        d.setdefault("current_floor_target_index", 0)
        d.setdefault("movement_speed_mm_per_tick", max(50, d.get("movement_speed_mm_per_tick", 200)))
        d["state"] = d.get("state", "MOVING")
        d["door_timer"] = d.get("door_timer", 0)
        d["is_door_open"] = d.get("is_door_open", False)
        d["first_moving_tick_sent_close"] = False

    def pick_next_target(device):
        # Randomly pick next floor target (30% random, else sequential)
        if random.random() < 0.3:
            device["current_floor_target_index"] = random.randint(0, len(device["floor_boundaries"]) - 1)
        else:
            device["current_floor_target_index"] = (device["current_floor_target_index"] + 1) % len(device["floor_boundaries"])

    def send_telemetry(device):
        # Send telemetry for a single device for one tick
        url = tb_url_for_token(device['token'])
        logs = []
        max_boundary = device['floor_boundaries'][-1]

        state = device["state"]
        current = device["current_height_mm"]
        target = device["floor_boundaries"][device["current_floor_target_index"]]
        speed = device["movement_speed_mm_per_tick"]

        def post_height_only():
            """
            Send periodic tick telemetry with current height and door state.
            Always includes door_val for rule chain logic.
            """
        # Handle MOVING state: move towards target, send door closed, open door on arrival
        # Handle DOOR_OPEN state: decrement timer, close door and pick next target when timer expires
        # Always send periodic tick telemetry
    # Main simulation loop: send telemetry for all devices every tick
            """Always include door_val on every tick."""
            door_state = "OPEN" if device["is_door_open"] else "CLOSE"
            payload = base_sensor_payload()
            payload.update({
                "laser_val": height_to_laser_val(device["current_height_mm"], max_boundary),
                "door_val": door_state
            })
            try:
                requests.post(url, json=payload, headers={'Content-Type': 'application/json'}, timeout=POST_TIMEOUT)
                logs.append(f"Height tick: {payload}")
            except Exception as e:
                logs.append(f"Height send error: {e}")

        if state == "MOVING":
            # Ensure door is marked closed when we start/continue moving
            if not device["first_moving_tick_sent_close"]:
                device["is_door_open"] = False  # keep flags consistent
                p = base_sensor_payload()
                p.update({
                    "door_val": "CLOSE",
                    "laser_val": height_to_laser_val(current, max_boundary)
                })
                send_json(url, p)
                device["first_moving_tick_sent_close"] = True

            # Move towards target
            if current < target:
                device["current_height_mm"] = min(current + speed, target)
            elif current > target:
                device["current_height_mm"] = max(current - speed, target)

            # Arrived at floor -> open door
            if abs(device["current_height_mm"] - target) <= speed * 0.001:
                device["current_height_mm"] = target
                device["state"] = "DOOR_OPEN"
                device["door_timer"] = random.randint(5, 10)
                device["is_door_open"] = True
                device["first_moving_tick_sent_close"] = False
                p = base_sensor_payload()
                p.update({
                    "door_val": "OPEN",
                    "laser_val": height_to_laser_val(device["current_height_mm"], max_boundary)
                })
                send_json(url, p)

        elif state == "DOOR_OPEN":
            device["door_timer"] -= 1
            if device["door_timer"] <= 0:
                # Close and start moving again
                device["is_door_open"] = False
                p = base_sensor_payload()
                p.update({
                    "door_val": "CLOSE",
                    "laser_val": height_to_laser_val(device["current_height_mm"], max_boundary)
                })
                send_json(url, p)
                pick_next_target(device)
                device["state"] = "MOVING"
                device["first_moving_tick_sent_close"] = True

        # Always send the periodic tick ninirirnoor_val included
        post_height_only()

        return "\n".join(logs)

    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(devices)) as executor:
            results = list(executor.map(send_telemetry, devices))
        print("=" * 50)
        print(f" Tick @ {time.strftime('%H:%M:%S')}")
        for i, output in enumerate(results):
            print(f"[Device {i+1:02d}] {output}")
        print("=" * 50)
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    # Entry point: prompt user for mode (full simulator or alarm tester)
    print("Select Mode:")
    print("1 - Run Full Simulator")
    print("2 - Trigger Alarm Tester")
    mode = input("Enter 1 or 2: ").strip()

    if mode == "1":
        run_full_simulator()
    elif mode == "2":
        run_alarm_tester()
    else:
        print("Invalid mode selection.")
