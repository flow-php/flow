<?php

declare(strict_types=1);

use Flow\Telemetry\Provider\Clock\SystemClock;
use function Flow\Telemetry\DSL\{
    console_log_exporter,
    logger_provider,
    memory_context_storage,
    memory_log_processor,
    resource_detector,
    telemetry
};

require __DIR__ . '/vendor/autoload.php';

$clock = new SystemClock();
$contextStorage = memory_context_storage();

$resource = resource_detector()->detect();

$telemetry = telemetry(
    $resource,
    null,
    null,
    logger_provider(
        memory_log_processor(console_log_exporter(colors: false, maxBodyLength: 200)),
        $clock,
        $contextStorage,
    ),
);

$telemetry->registerShutdownFunction();

$logger = $telemetry->logger('order-service');

$logger->info('Application started', [
    'environment' => 'development',
]);

$logger->debug('Processing batch', [
    'batch.size' => 100,
    'batch.id' => 'B-001',
]);

$logger->warn('Slow query detected', [
    'query.duration_ms' => 5000,
    'query.table' => 'orders',
]);

$logger->error('Connection failed', [
    'service.name' => 'payment-gateway',
    'error.code' => 'TIMEOUT',
]);
