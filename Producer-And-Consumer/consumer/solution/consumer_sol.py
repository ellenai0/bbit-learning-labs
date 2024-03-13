import pika
import os
import sys
from pathlib import Path
from Path.parent.consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
        self.connection = None
        self.channel = None
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])

        self.connection = pika.BlockingConnection(parameters=conParams)

        # Establish Channel

        self.channel = connection.channel()

        # Create Queue if not already present
        channel.queue_declare(queue=self.queue_name)

        # Create the exchange if not already present
        channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')
        
        
        # Bind Binding Key to Queue on the exchange
        channel.exchangeBind(self.binding_key)

        # Set-up Callback function for receiving messages
        channel.basic_consume(queue='demo', on_message_callback=on_message_callback, auto_ack=True)
        
    def on_message_callback(self, channel, method_frame, header_frame, body):
        # Acknowledge message


        #Print message (The message is contained in the body parameter variable)
        print(f" [x] Received {body}")

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.channel.start_consuming()

    
    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()