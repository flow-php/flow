Structured logging with Flow Telemetry.

The Logger emits log records with severity levels and supports structured attributes.

## Severity Levels

```php
$logger->trace('Detailed trace info');
$logger->debug('Debug information');
$logger->info('Informational message');
$logger->warn('Warning condition');
$logger->error('Error condition');
$logger->fatal('Fatal error');
```

## Structured Attributes

```php
$logger->info('Order processed', [
    'order.id' => 'ORD-123',
    'items.count' => 5,
    'total.amount' => 299.99,
]);
```
