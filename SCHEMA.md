
# SCHEMA.md – Cassandra Schema for Messenger App

This document outlines the design of the Cassandra database used to power features like sending messages, maintaining user conversations, and supporting pagination.

---

## Keyspace: `messenger`

The **keyspace** is the top-level namespace in Cassandra that defines how data is replicated across the nodes in the cluster.

```sql
CREATE KEYSPACE IF NOT EXISTS messenger WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

- **SimpleStrategy**: Used for single data center setups.
- **replication_factor = 1**: Only one replica is maintained per piece of data (suitable for development/testing).

---

## Tables Overview

The schema includes four key tables, each serving a specific purpose in the messaging workflow:

---

### 1. `indexes`

Used for generating **sequential IDs** (like auto-incrementing values) using Cassandra’s **counter type**.

```sql
CREATE TABLE IF NOT EXISTS indexes (
    index_name TEXT,
    index_value COUNTER,
    PRIMARY KEY (index_name)
);
```

- `index_name`: Name of the index (e.g., `message_id` or `conversation_id`).
- `index_value`: Counter that auto-increments on each update.

---

### 2. `messages`

Stores **individual messages** exchanged in conversations.

```sql
CREATE TABLE IF NOT EXISTS messages (
    conversation_id INT,
    sender_id INT,
    receiver_id INT,
    timestamp TIMESTAMP,
    message_id INT,
    content TEXT,
    PRIMARY KEY (conversation_id, timestamp, message_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, message_id ASC);
```

- **Partition key**: `conversation_id` – ensures all messages of a conversation are stored together.
- **Clustering columns**: `timestamp`, `message_id` – allows sorting messages by most recent (`DESC`) and resolving collisions using `message_id`.

> Ideal for fetching the latest messages and implementing **pagination**.

---

### 3. `user_conversations`

Tracks the participants involved in each conversation and stores metadata about the **last message**.

```sql
CREATE TABLE IF NOT EXISTS user_conversations (
    sender_id INT,
    receiver_id INT,
    conversation_id INT,
    last_timestamp TIMESTAMP,
    last_message TEXT,
    PRIMARY KEY (conversation_id)
);
```

- Quickly identifies participants.
- Stores the **last exchanged message** for UI previews.

---

### 4. `conversations`

Enables a **user-centric view** of their conversations, showing the **most recent ones first**.

```sql
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id INT,
    sender_id INT,
    receiver_id INT,
    last_timestamp TIMESTAMP,
    PRIMARY KEY (conversation_id, sender_id)
);
```

- Enables listing a user’s active conversations.
- Designed for fast querying by `sender_id` and sorting by `last_timestamp`.

---

## Summary

| Table               | Purpose                                          | Query Optimization       |
|--------------------|--------------------------------------------------|--------------------------|
| `indexes`          | Sequential ID generation                         | Efficient ID tracking    |
| `messages`         | Store and paginate messages                      | Recent-first retrieval   |
| `user_conversations` | Track users in each conversation + last message | Conversation metadata    |
| `conversations`    | Fetch user's latest conversations                | Fast lookup by user      |

---

## Notes

- All tables are dropped and recreated during initialization to ensure a fresh state.
- Suitable for **chat apps**, **social platforms**, or **customer support tools**.

---
