-- Cassandra keyspace: NetworkTopologyStrategy with RF=3 for 3-node cluster.
-- dc1 matches the CASSANDRA_DC env variable set in docker-compose.yml.
CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'dc1': 3
}
AND durable_writes = true;
