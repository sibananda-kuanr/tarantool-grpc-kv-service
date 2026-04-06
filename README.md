# VK KV gRPC Service

A Java gRPC service that provides key-value storage backed by **Tarantool 3.2.x**.

## API

| Method | Type | Description |
|--------|------|-------------|
| `Put(key, value)` | Unary | Stores value for key — inserts or overwrites |
| `Get(key)` | Unary | Returns value for the specified key |
| `Delete(key)` | Unary | Deletes the value for the specified key |
| `Range(key_since, key_to)` | Server-streaming | Returns key-value pairs in range |
| `Count()` | Unary | Returns number of records in the database |

## Requirements

- Java 17+
- Docker & Docker Compose
- Maven 3.6+

## Tech Stack

- **gRPC Java** 1.62.2
- **Tarantool Java SDK** `org.tarantool:connector:1.6.0`
- **Tarantool 3.2.x** (Docker)
- **Maven** for build

## Tarantool KV Space Schema

```lua
{name = 'key',   type = 'string'},
{name = 'value', type = 'varbinary', is_nullable = true}
```

Primary index: TREE on `key` (ordered, supports efficient range queries).

## Running

### 1. Start Tarantool

```bash
docker-compose up -d
```

### 2. Build

```bash
mvn clean package -DskipTests
```

### 3. Run the gRPC Service

```bash
java -jar target/kv-service-1.0.0.jar
```

Service starts on port **9090**. Tarantool is expected on `localhost:3301`.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TARANTOOL_HOST` | `localhost` | Tarantool host |
| `TARANTOOL_PORT` | `3301` | Tarantool port |
| `GRPC_PORT` | `9090` | gRPC server port |

## Testing with grpcurl

```bash
# Install grpcurl first: https://github.com/fullstorydev/grpcurl

# Put a value
grpcurl -plaintext -proto proto/kv_service.proto \
  -d '{"key":"hello","value":"d29ybGQ="}' \
  localhost:9090 kvservice.KvService/Put

# Get a value
grpcurl -plaintext -proto proto/kv_service.proto \
  -d '{"key":"hello"}' \
  localhost:9090 kvservice.KvService/Get

# Delete
grpcurl -plaintext -proto proto/kv_service.proto \
  -d '{"key":"hello"}' \
  localhost:9090 kvservice.KvService/Delete

# Range
grpcurl -plaintext -proto proto/kv_service.proto \
  -d '{"key_since":"a","key_to":"z"}' \
  localhost:9090 kvservice.KvService/Range

# Count
grpcurl -plaintext -proto proto/kv_service.proto \
  -d '{}' \
  localhost:9090 kvservice.KvService/Count
```

## Notes

- `put` and `get` work correctly with **null values** in the `value` field.
- Methods work correctly with **5,000,000 records** in the space (TREE index, streaming range).
- The `range` method uses server-side streaming to efficiently handle large result sets.
