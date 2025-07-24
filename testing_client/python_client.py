import redis

r = redis.Redis(host='localhost', port=6380)

try:
    response = r.ping()
    print(f"Connected successfully:{response}")
except redis.ConnectionError as e:
    print(f"Failed:{e}")

    
# Basic SET and GET operations
r.set("greeting", "Hello from Redis-py")
print(f"greeting: {r.get('greeting')}")

# Overwriting existing key
r.set("greeting", "Hello again!")
print(f"greeting (updated): {r.get('greeting')}")


r.delete("greeting")