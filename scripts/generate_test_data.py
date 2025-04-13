"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10
NUM_CONVERSATIONS = 15
MAX_MESSAGES_PER_CONVERSATION = 50

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    This function creates:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")

    # Generate conversations
    for conversation_id in range(1, NUM_CONVERSATIONS + 1):
        # Randomly select two distinct users for the conversation
        user1_id = random.randint(1, NUM_USERS)
        user2_id = random.randint(1, NUM_USERS)
        while user2_id == user1_id:
            user2_id = random.randint(1, NUM_USERS)

        # Generate a random timestamp for the last message
        last_timestamp = datetime.now() - timedelta(days=random.randint(0, 30))

        # Insert the conversation into the `user_conversations` table
        session.execute(
            """
            INSERT INTO user_conversations (conversation_id, sender_id, receiver_id, last_timestamp, last_message)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (conversation_id, user1_id, user2_id, last_timestamp, f"Last message in conversation {conversation_id}")
        )

        # Generate a random number of messages for the conversation
        num_messages = random.randint(1, MAX_MESSAGES_PER_CONVERSATION)
        for message_id in range(1, num_messages + 1):
            # Generate a random timestamp for the message
            timestamp = last_timestamp - timedelta(minutes=random.randint(1, 1000))

            # Randomly assign the sender and receiver for the message
            sender_id = user1_id if random.choice([True, False]) else user2_id
            receiver_id = user2_id if sender_id == user1_id else user1_id

            # Insert the message into the `messages` table
            session.execute(
                """
                INSERT INTO messages (conversation_id, sender_id, receiver_id, timestamp, message_id, content)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conversation_id, sender_id, receiver_id, timestamp, message_id, f"Message {message_id} in conversation {conversation_id}")
            )

    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 