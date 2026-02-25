Context propagation across service boundaries.

Propagators extract and inject trace context using standard headers. W3C Trace Context (`traceparent`, `tracestate`) carries trace IDs. W3C Baggage carries application-specific data.

## Extract Context from Request

```php
$propagator = composite_propagator(w3c_trace_context(), w3c_baggage());
$carrier = array_carrier($incomingHeaders);
$context = $propagator->extract($carrier);
```

## Inject Context into Response

```php
$outCarrier = array_carrier();
$propagator->inject($context, $outCarrier);
$outgoingHeaders = $outCarrier->unwrap();
```

## Header Format

```
traceparent: 00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01
baggage: userId=alice,requestId=req-123
```
