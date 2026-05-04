import math

def classify_vibration(score):
    if score <= 0.03:
        return "Stationary"
    elif score <= 0.08:
        return "Mild Vibration"
    elif score <= 0.20:
        return "Strong Vibration"
    else:
        return "Shock/Impact"


def process_vibration(msg, metadata=None, msgType=None):
    if metadata is None:
        metadata = {}

    # Read accelerometer values
    x = float(msg.get("x", 0))
    y = float(msg.get("y", 0))
    z = float(msg.get("z", 0))

    # Calculate acceleration magnitude
    acc_mag = math.sqrt(x*x + y*y + z*z)

    # Get previous value (fallback to current if not present)
    prev_acc_mag = float(metadata.get("prev_acc_mag", acc_mag))

    # Calculate vibration score
    vibration_score = abs(acc_mag - prev_acc_mag)

    # Threshold (default = 0.08)
    threshold = float(metadata.get("vibration_threshold", 0.08))

    # Check vibration
    is_vibrating = vibration_score > threshold

    # Round values
    acc_mag_rounded = round(acc_mag, 4)
    vibration_score_rounded = round(vibration_score, 4)

    # Add results to msg
    msg["acc_mag"] = acc_mag_rounded
    msg["vibration_score"] = vibration_score_rounded
    msg["is_vibrating"] = is_vibrating

    # ✅ Add classification
    msg["vibration_level"] = classify_vibration(vibration_score_rounded)

    return {
        "msg": msg,
        "metadata": metadata,
        "msgType": msgType
    }