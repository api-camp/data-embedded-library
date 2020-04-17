# Api.Camp::Data SDK

Library to interact with low latency Key/Value stores meant to be embedded with applications and run on commodity
hardware.

## Use Case: Embedded In Memory Store

Leverages a `TreeMap`, i.e. `O(log(n))` and store all data in memory. Be careful of heap pressure.

```java
String namespace = "MemSpace";
KeyValueStore<MyModelType> store = MapKeyValueStore.create(namespace);

store.put("message", MyModelType.of("Hello, World"));
log.info("key('message')=>{}", store.get("message").orElse(null));
```

## Use Case: Embedded On Disk

Simple implementation that is persistent across restarts.

```java
String namespace = "MemSpace";
KeyValueStore<MyModelType> store = RocksKeyValueStore.create(namespace);

store.put("message", MyModelType.of("Hello, World"));
log.info("key('message')=>{}", store.get("message").orElse(null));
```

By default the data will be persisted into `./__db/<namespace>/db.rocks` relative to the working directory.