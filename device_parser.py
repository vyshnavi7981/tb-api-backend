import pandas as pd
import random

def parse_device_config(csv_path):
    df = pd.read_csv(csv_path)

    
    required_columns = ['access_token', 'floor_boundaries']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    device_states = []

    for idx, row in df.iterrows():
        token = str(row['access_token']).strip()
        try:
            floor_boundaries = [int(x.strip()) for x in str(row['floor_boundaries']).split(',')]
        except Exception as e:
            print(f"Skipping row {idx} due to malformed floor boundaries: {e}")
            continue

        if len(floor_boundaries) < 2:
            print(f"Skipping row {idx}: not enough floor boundaries.")
            continue

        
        movement_speed = 200 + random.choice([-20, 0, 20])
        initial_floor_index = random.randint(0, len(floor_boundaries) - 2)
        initial_height = floor_boundaries[initial_floor_index] + random.randint(0, movement_speed - 1)

        device_states.append({
            "token": token,
            "floor_boundaries": floor_boundaries,
            "current_height_mm": initial_height,
            "current_floor_target_index": random.randint(0, len(floor_boundaries) - 1),
            "movement_speed_mm_per_tick": movement_speed,
            "is_door_open": False,
            "door_timer": 0,
            "previous_door_state": False,
            "last_temp_humidity_sent": 0,
        })

    return device_states
