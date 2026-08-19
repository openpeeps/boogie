# Description: Persistent FIFO queue or append-only log.
# How: Table with auto-increment PK, payload, and status. APIs for enqueue, dequeue, ack.
# Example: Kafka-lite, Redis Streams.


## Summary Table

# | Store Type         | Example Use Case         | Implementation Notes                |
# |--------------------|-------------------------|-------------------------------------|
# | Relational (store) | General purpose         | Add relations, joins, indexes       |
# | Key-Value          | Caching, configs        | 1 table, key+value                  |
# | Vector Store       | Embeddings, ANN search  | Already implemented                 |
# | Document Store     | JSON docs, flexible     | Store JSON, per-doc schema          |
# | Time Series        | Metrics, logs           | Timestamp, value, tags              |
# | Graph Store        | Social, networks        | Nodes/edges tables, traversals      |
# | Full-Text Search   | Search, logs            | Inverted index, text columns        |
# | Queue/Log Store    | Messaging, events       | Append-only, ack, dequeue           |
# | Columnar Store     | Analytics, OLAP         | Column-wise storage, compression    |