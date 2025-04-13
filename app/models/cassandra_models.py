"""
Models for interacting with Cassandra tables in the Facebook Messenger backend project.
"""
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
 
from app.db.cassandra_client import cassandra_client
import logging
from cassandra.query import SimpleStatement

logger = logging.getLogger(__name__)
 
class MessageModel:
    """
    Message model for interacting with the messages table.
    """
 
    @staticmethod
    async def create_message(conversation_id: int, sender_id: int, receiver_id: int, content: str) -> Dict[str, Any]:
        """
        Create a new message.
 
        Args:
            sender_id (int): ID of the sender
            receiver_id (int): ID of the receiver
            content (str): Content of the message
 
        Returns:
            dict: Details of the created message matching MessageResponse schema
        """
        # First, get or create a conversation between these users

        # Get the next message ID
        msg_id_query = "SELECT index_value FROM indexes WHERE index_name = 'message_id'"
        output = await cassandra_client.execute(msg_id_query)
        msg_id = output[0]["index_value"] + 1 if output else 1
        logger.info(f"Message ID: {msg_id}")
        # Update the counter
        await cassandra_client.execute(
            "UPDATE indexes SET index_value = index_value + 1 WHERE index_name = 'message_id'"
        )
 
        created_at = datetime.now()
 
        # Insert into messages table
        query = """
        INSERT INTO messages (message_id, conversation_id, sender_id, receiver_id, content, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        await cassandra_client.execute(query, (msg_id, conversation_id, sender_id, receiver_id, content, created_at))
 
        check_conversation_query = """
            select conversation_id from user_conversations where conversation_id = %s 
            """
        rows = await cassandra_client.execute(check_conversation_query, (conversation_id,))
 
        if not rows.current_rows:
            logger.info("No existing conversation found.")
        else:
            for row in rows:
                logger.info(f"Checking if conversation already exists: {row}")
 
        if not rows:
            # If conversation doesn't exist, create it
            insert_query = """
            INSERT INTO user_conversations (conversation_id, sender_id, receiver_id, last_timestamp, last_message)
            VALUES (%s, %s, %s, %s, %s)
            """
            await cassandra_client.execute(insert_query, (conversation_id, sender_id, receiver_id, created_at, content))
        else:
            # If conversation exists, update it with the new message
            update_query = """
            UPDATE user_conversations SET last_timestamp = %s, last_message = %s, sender_id = %s, receiver_id = %s WHERE conversation_id = %s
            """
            await cassandra_client.execute(update_query, (created_at, content, sender_id, receiver_id, conversation_id))

        return {
            "message_id": msg_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "timestamp": created_at,
            "conversation_id": conversation_id
        }
 
    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get messages for a conversation with pagination.
 
        Args:
            conversation_id (int): ID of the conversation
            page (int): Page number for pagination (default: 1)
            limit (int): Number of messages per page (default: 20)
 
        Returns:
            tuple: (List of messages, Total count) for PaginatedMessageResponse
        """
        # Get total count of messages in the conversation
        count_query = """
        SELECT COUNT(*) as count FROM messages WHERE conversation_id = %s
        """
        count_output = await cassandra_client.execute(count_query, (conversation_id,))
        total = count_output[0]["count"] if count_output else 0
 
        # Get messages with pagination
        query = """
        SELECT message_id, sender_id, receiver_id, content, timestamp
        FROM messages
        WHERE conversation_id = %s
        ORDER BY timestamp DESC 
        """
 
        output = await cassandra_client.execute(query, (conversation_id,))
 
        messages = []
        for row in output:
            messages.append({
                "id": row["message_id"],
                "sender_id": row["sender_id"],
                "receiver_id": row["receiver_id"],
                "content": row["content"],
                "created_at": row["timestamp"],
                "conversation_id": conversation_id
            })
        total = len(messages)
        offset = (page - 1) * limit
        paginated_messages = messages[offset:offset + limit]
 
        messages = paginated_messages if paginated_messages else []
        return messages, total
 
    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int, 
        before_timestamp: datetime, 
        page: int = 1, 
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get messages before a timestamp with pagination.
 
        Args:
            conversation_id (int): ID of the conversation
            before_timestamp (datetime): Timestamp to filter messages
            page (int): Page number for pagination (default: 1)
            limit (int): Number of messages per page (default: 20)
 
        Returns:
            tuple: (List of messages, Total count) for PaginatedMessageResponse
        """
        # Get total count of messages before the timestamp
        count_query = """
        SELECT COUNT(*) as count FROM messages 
        WHERE conversation_id = %s AND timestamp < %s
        """
        count_output = await cassandra_client.execute(count_query, (conversation_id, before_timestamp))
        total = count_output[0]["count"] if count_output else 0
 
        # Get messages before timestamp with pagination
        query = """
        SELECT message_id, sender_id, receiver_id, content, timestamp
        FROM messages
        WHERE conversation_id = %s AND timestamp < %s
        ORDER BY timestamp DESC
        """
        output = await cassandra_client.execute(query, (conversation_id, before_timestamp))
 
        messages = []
        for row in output:
            messages.append({
                "id": row["message_id"],
                "sender_id": row["sender_id"],
                "receiver_id": row["receiver_id"],
                "content": row["content"],
                "created_at": row["timestamp"],
                "conversation_id": conversation_id
            })
 
        total = len(messages)
        offset = (page - 1) * limit
        paginated_messages = messages[offset:offset + limit]
 
        messages = paginated_messages if paginated_messages else []
 
        return messages, total

class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    """
 
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
 
        # Get conversations with pagination
        query_one = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM user_conversations 
        WHERE sender_id = %s 
        ALLOW FILTERING       
        """
 
        query_two = """
        SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
        FROM user_conversations 
        WHERE receiver_id = %s
        ALLOW FILTERING
        """
        output_one = await cassandra_client.execute(query_one, (user_id,))
        output_two = await cassandra_client.execute(query_two, (user_id,))
 
        output_list_one = list(output_one)
        output_list_two = list(output_two)
        output_list_one.extend(output_list_two)
        sorted(output_list_one, key=lambda x: x["last_timestamp"])
 
        logger.info(f"Rows fetched: {output_list_one}")
 
        total = len(output_list_one)
        offset = (page - 1) * limit
        paginated_output = output_list_one[offset:offset + limit]
 
        conversations = []
 
        for row in paginated_output:
            logger.info(f"Checking if conversation already exists: {row}")
            conversations.append({
                "id": row["conversation_id"],
                "user1_id": row["sender_id"],
                "user2_id": row["receiver_id"],
                "last_message_at": row["last_timestamp"],
                "last_message_content": row["last_message"]
            })

        return conversations, total
 
    @staticmethod
    async def create_conversation(sender_id: int, receiver_id: int):
        try:
           #getting the conversation id by running the indexes
            conversation_id_query = "SELECT index_value FROM indexes WHERE index_name = 'conversation_id'"
            output = await cassandra_client.execute(conversation_id_query)
            conversation_id = output[0]["index_value"] + 1 if output else 1
 
            #Update the counter
            await cassandra_client.execute(
                "UPDATE indexes SET index_value = index_value + 1 WHERE index_name = 'conversation_id'"
            )
 
            created_at = datetime.now()
 
            insert_query = """
                INSERT INTO conversations (conversation_id, sender_id, receiver_id, last_timestamp)
                VALUES (%s, %s, %s, %s)
            """
 
            await cassandra_client.execute(insert_query, (conversation_id, sender_id, receiver_id, created_at))
 
            return {
                "conversation_id": conversation_id,
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "created_at": created_at
            }
 
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Database error: {str(e)}"
            )
 
 
    @staticmethod
    async def get_conversation(conversation_id: int) -> Dict[str, Any]:
        """
        Get a conversation by ID.
 
        Args:
            conversation_id (int): ID of the conversation
 
        Returns:
            dict: Details of the conversation matching ConversationResponse schema
        """
        query = """
                SELECT conversation_id, sender_id, receiver_id, last_timestamp, last_message
                FROM user_conversations 
                WHERE conversation_id = %s
                """
        output = await cassandra_client.execute(query, (conversation_id,))
 
        if not output:
            return None
 
        result = output[0]

        return {
            "conversation_id": result["conversation_id"],
            "sender_id": result["sender_id"],
            "receiver_id": result["receiver_id"],
            "last_message_at": result["last_timestamp"],
            "last_message_content": result["last_message"]
        }
 
    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.
 
        Args:
            user1_id (int): ID of the first user
            user2_id (int): ID of the second user
 
        Returns:
            dict: Details of the conversation matching ConversationResponse schema
        """
 
        # Check if the conversation already exists
        query_one = """
                SELECT conversation_id FROM conversations 
                WHERE sender_id = %s AND receiver_id = %s 
                ALLOW FILTERING
                """
        output_one = await cassandra_client.execute(query_one, (user1_id, user2_id))
 
        query_two = """
                SELECT conversation_id FROM conversations 
                WHERE sender_id = %s AND receiver_id = %s 
                ALLOW FILTERING
                """
        output_two = await cassandra_client.execute(query_two, (user2_id, user1_id))
 
 
        if output_one:
            return await ConversationModel.get_conversation(output_one[0]["conversation_id"])
        if output_two:
            return await ConversationModel.get_conversation(output_two[0]["conversation_id"])
 
        # Get the next conversation ID
        conversation_id_query = "SELECT index_value FROM indexes WHERE index_name = 'conversation_id'"
        output = await cassandra_client.execute(conversation_id_query)
        conversation_id = output[0]["index_value"] + 1 if output else 1
 
        # Update the counter
        await cassandra_client.execute(
            "UPDATE indexes SET index_value = index_value + 1 WHERE index_name = 'conversation_id'"
        )
 
        created_at = datetime.now()
 
        # Insert into conversations table
        insert_query = """
        INSERT INTO conversations (conversation_id, sender_id, receiver_id, last_timestamp)
        VALUES (%s, %s, %s, %s)
        """
        await cassandra_client.execute(insert_query, (conversation_id, user1_id, user2_id, created_at))

        return {
            "conversation_id": conversation_id,
            "sender_id": user1_id,
            "receiver_id": user2_id,
            "last_message_at": created_at,
            "last_message_content": None
        }