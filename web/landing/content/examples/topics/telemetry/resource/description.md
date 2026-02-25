Combining resource detection with custom attributes.

The `resource_detector()` function auto-detects OS, host, process, and service information. You can merge detected attributes with custom ones using `resource()->merge()`.

## Detect System Attributes

```php
$detected = resource_detector()->detect();
```

## Add Custom Attributes

```php
$custom = resource([
    'deployment.environment.name' => 'production',
    'service.instance.id' => 'worker-01',
]);
```

## Merge Resources

Custom attributes take precedence when merging:

```php
$resource = $detected->merge($custom);
```
