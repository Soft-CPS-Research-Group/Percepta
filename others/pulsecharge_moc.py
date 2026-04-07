import pika
import json
import random
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

# RabbitMQ settings
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5673
EXCHANGE_NAME = 'building_i-charging_headquarters'
EXCHANGE_TYPE = 'fanout'
EXCHANGE_DURABLE = True
USERNAME = 'dataprovider'
PASSWORD = 'dataprovidermq'

data_list = [
    {"soc": 0.53, "vin": "VR3UKZKXZNJ855677", "estimated_soc_at_arrival": 0.75, "estimated_soc_at_departure": 1, "estimated_time_at_arrival": "20:00", "estimated_time_at_departure": "10:00", "mode": "FLEX_BASE"},
    {"soc": 0.53, "vin": "LSJWH4099PN266007", "estimated_soc_at_arrival": 0.75, "estimated_soc_at_departure": 1, "estimated_time_at_arrival": "20:00", "estimated_time_at_departure": "10:00", "mode": "FLEX_BASE"},
    {"soc": 0.53, "vin": "WVWZZZE1ZPP010253", "estimated_soc_at_arrival": 0.75, "estimated_soc_at_departure": 1, "estimated_time_at_arrival": "20:00", "estimated_time_at_departure": "10:00", "mode": "FLEX_BASE"}]

# Persistent connection and channel
credentials = pika.PlainCredentials(USERNAME, PASSWORD)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=EXCHANGE_DURABLE)

def update_soc(data_list):
    for vehicle in data_list:
        vehicle['soc'] = max(0, min(1, vehicle['soc'] + random.uniform(-0.01, 0.01)))

def send_message():
    update_soc(data_list)
    for dict in data_list:
        message = json.dumps(dict)
        try:
            channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='', body=message)
        except pika.exceptions.AMQPError as e:
            print("Failed to send message:", e)

scheduler = BackgroundScheduler()
scheduler.add_job(send_message, 'cron', second=0, misfire_grace_time=10)

def start():
    scheduler.start()
    print("Scheduler started. Messages will be sent at the beginning of each minute.")
