# import requests
# import time
# import json
# from datetime import datetime, timedelta
# import paho.mqtt.client as mqtt

# # GPS TrackTech API details
# URL = "https://y.gpstracktech.com/webapi/home/todayAlarm"
# TOKEN = "U7550-8e6fef91-695b-4f68-9678-89b65e17ff80"

# # MQTT client setup
# mqtt_client = mqtt.Client()
# mqtt_client.username_pw_set("NPo2MuxdEZb0hNYN1WAd")  # If your broker requires auth
# mqtt_client.connect("sensify.techosec.com", 1883, 60)  # Replace with your MQTT broker info
# mqtt_client.loop_start()

# def fetch_and_push_mqtt():
#     while True:
#         end_time = datetime.now()
#         start_time = end_time - timedelta(seconds=5)
#         params = {
#             "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
#             "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
#             "__sm_ver": "1.22.2,20250904190041",
#             "platform": "PC",
#             "version": "base"
#         }
#         headers = {"accept": "application/json", "token": TOKEN}

#         try:
#             response = requests.get(URL, headers=headers, params=params)
#             if response.status_code == 200:
#                 data = response.json()
#                 adas_list = data.get('data', {}).get('adasList', [])

#                 # Transform to a dictionary: {alarmName: total}
#                 transformed = {alarm.get("alarmName"): alarm.get("total") for alarm in adas_list}

#                 # Build payload similar to your example
#                 payload = {

#                     "adasAlarms": transformed
#                 }

#                 # Publish payload to MQTT
#                 mqtt_client.publish("v1/devices/me/telemetry", json.dumps(payload))
#                 print(f"Sent telemetry: {payload}")

#             else:
#                 print(f"Error {response.status_code}: {response.text}")
#         except Exception as e:
#             print("Exception while fetching data:", e)

#         time.sleep(5)

# # Start fetching
# fetch_and_push_mqtt()

import requests
from datetime import datetime, timedelta
import time
import pandas as pd
import json
import paho.mqtt.client as mqtt

# GPS TrackTech API details
URL = "https://y.gpstracktech.com/webapi/home/todayAlarm"
TOKEN = "U7550-8e6fef91-695b-4f68-9678-89b65e17ff80"

# MQTT / ThingsBoard configuration
THINGSBOARD_HOST = "sensify.techosec.com"
# DEVICE_TOKEN = "NPo2MuxdEZb0hNYN1WAd"
DEVICE_TOKEN = "f1YbPn2Hfg3Ab2WEPgbA"
MQTT_PORT = 1883
MQTT_TOPIC = "v1/devices/me/telemetry"

# List of all ADAS alarms
ALL_ALARMS = [
    "SOS(Adas)", "Vibration(Adas)", "SOS(IPC)", "Fuel Theft", "Refueling",
    "Rollover", "Watch for Pedestrians", "Forward Collision", "Road Departure",
    "Pedestrian Collision", "Front Car Close Distance", "Road Sign Recognition Event",
    "Active Capture", "Driver Assistance Function Failure", "Harsh Acceleration(Adas)",
    "Harsh Braking(Adas)", "Harsh Cornering(Adas)", "Engine Start(Adas)",
    "Engine OFF(Adas)", "OverSpeed(Adas)", "Human Detection", "Left Rear Approach",
    "Right Rear Approach", "Forward Approach", "Rear Approach"
]

# MQTT client setup
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(DEVICE_TOKEN)
mqtt_client.connect(THINGSBOARD_HOST, MQTT_PORT, 60)
mqtt_client.loop_start()  # start background loop

def fetch_and_push_mqtt():
    while True:
        end_time = datetime.now()
        start_time = end_time - timedelta(seconds=5)

        params = {
            "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "__sm_ver": "1.22.2,20250904190041",
            "platform": "PC",
            "version": "base"
        }
        headers = {"accept": "application/json", "token": TOKEN}

        try:
            response = requests.get(URL, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                adas_list = data.get('data', {}).get('adasList', [])

                # Create dict {alarmName: total} and ensure all alarms are present
                api_dict = {alarm.get("alarmName"): alarm.get("total") for alarm in adas_list}
                full_dict = {alarm: api_dict.get(alarm, 0) for alarm in ALL_ALARMS}
                full_dict["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Publish to ThingsBoard MQTT
                mqtt_client.publish(MQTT_TOPIC, json.dumps(full_dict))
                print(f"Sent telemetry: {full_dict}")

            else:
                print(f"Error {response.status_code}: {response.text}")
        except Exception as e:
            print("Exception while fetching data:", e)

        time.sleep(5)

# Start fetching and pushing
fetch_and_push_mqtt()
