-- Upgrade script from pg_redis_core 0.2.0 to 0.3.0
-- Adds in-process Redis Pub/Sub (PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PUBSUB, RESET)
-- and PostgreSQL 16 compatibility. No catalog table schema changes required.
SELECT 1;
