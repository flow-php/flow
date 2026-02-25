Distributed tracing with Flow Telemetry.

The Tracer creates spans representing operations. Spans can be nested to show parent-child relationships and include attributes, events, and status.

## Creating Spans

```php
$span = $tracer->span('operation-name', SpanKind::INTERNAL);
$span->setAttribute('key', 'value');
$tracer->complete($span);
```

## Span Kinds

- `INTERNAL` - Internal operation (default)
- `CLIENT` - Outgoing request to external service
- `SERVER` - Incoming request handler
- `PRODUCER` - Message producer
- `CONSUMER` - Message consumer
