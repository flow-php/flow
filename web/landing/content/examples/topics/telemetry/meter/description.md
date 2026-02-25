Metrics collection with Flow Telemetry.

The Meter creates instruments for recording numeric measurements.

## Instrument Types

**Counter** - Monotonically increasing (requests, errors):
```php
$counter = $meter->createCounter('http.requests', 'requests');
$counter->add(1, ['method' => 'GET']);
```

**UpDownCounter** - Values that go up and down (queue size):
```php
$gauge = $meter->createUpDownCounter('queue.size', 'items');
$gauge->add(5);
$gauge->add(-2);
```

**Histogram** - Distribution of values (latency):
```php
$histogram = $meter->createHistogram('http.duration', 'ms');
$histogram->record(42.5, ['status' => 200]);
```

**Gauge** - Current value at a point in time:
```php
$gauge = $meter->createGauge('memory.usage', 'bytes');
$gauge->record(memory_get_usage());
```
