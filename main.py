from aws_mqtt import MQTTConnection
import config
import time

def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print(f"Received message from topic '{topic}': {payload}")

def main():
    mqtt_connection = MQTTConnection()
    mqtt_connection.connect()
    mqtt_connection.subscribe(config.SUB_TOPIC, on_message_received)

    while True:
        print(f'Publishing message on topic {config.PUB_TOPIC}')
        hello_world_message = {
            'message': f'Hello from {config.THING_NAME} in the AWS IoT Workshop'
        }
        mqtt_connection.publish(config.PUB_TOPIC, hello_world_message)
        time.sleep(5)

if __name__ == "__main__":
    main()