import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import pika
import json
import uuid

# --- Configuration (Factor III) ---
# Load environment variables (e.g., RABBITMQ_URL, PORT) from a local .env file
load_dotenv()

# Get the Message Queue connection URL from the environment
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/%2F')
PORT = os.getenv("PORT", 5000)

# --- Application Initialization ---
app = Flask(__name__)
CORS(app) # Enable Cross-Origin Resource Sharing (CORS)

# --- Asynchronous Communication (Publish/Subscribe) ---
def publish_order_event(order_id: str, order_data: dict):
    """
    Connects to RabbitMQ and publishes an 'order_created' event.
    """
    try:
        # Create Pika connection parameters from the environment URL
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        # Declare the 'fanout' exchange (event topic)
        exchange_name = 'order_events'
        channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

        event_message = {
            "event": "order_created",
            "order_id": order_id,
            "details": order_data
        }

        # Publish the message. 'fanout' means all queues bound to the exchange will receive it.
        channel.basic_publish(
            exchange=exchange_name,
            routing_key='',
            body=json.dumps(event_message).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.DeliveryMode.Transient
            )
        )
        app.logger.info(f"Published order_created event for ID: {order_id}")

    except pika.exceptions.AMQPConnectionError as e:
        app.logger.warning(f"Could not connect to message broker: {e}. Event was not published.")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()


# --- API Endpoint: POST /orders ---
@app.route('/orders', methods=['POST'])
def create_order():
    """
    Handles an incoming request to place a new order.
    """
    try:
        order_data = request.get_json()
        
        # 1. Generate unique Order ID
        order_id = str(uuid.uuid4())
        
        # --- NOTE: In a real app, you would save this order_data to a database here (Database-per-Service). ---
        
        # 2. Publish the event asynchronously
        publish_order_event(order_id, order_data)
        
        # 3. Return immediate acceptance to the client (synchronous response)
        return jsonify({
            "message": "Order placed and is being processed.",
            "order_id": order_id,
            "status": "ACCEPTED"
        }), 201

    except Exception as e:
        app.logger.error(f"Error processing order: {e}")
        return jsonify({"message": "Internal server error"}), 500

# --- Health Check (Standard for Cloud-Native) ---
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "OK", "service": "order-service"}), 200


# --- Application Runner (Factor VI: Processes) ---
if __name__ == '__main__':
    # Run the application using the port from the environment variable (or 5000 default)
    print(f" * Order Service starting on http://127.0.0.1:{PORT}")
    app.run(debug=True, port=PORT)
