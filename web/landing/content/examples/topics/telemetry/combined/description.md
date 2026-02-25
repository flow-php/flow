Complete observability with all three telemetry pillars.

This example demonstrates using Tracer, Logger, and Meter together for full observability of an operation.

## The Three Pillars

**Traces** - Track request flow across operations:
```php
$span = $tracer->span('process-order', SpanKind::INTERNAL);
```

**Logs** - Record events with context:
```php
$logger->info('Processing order', ['order.id' => $orderId]);
```

**Metrics** - Measure performance and counts:
```php
$counter->add(1, ['status' => 'success']);
$histogram->record($duration);
```

## Correlation

All three signals share the same resource and context, enabling correlation across traces, logs, and metrics in observability backends.
