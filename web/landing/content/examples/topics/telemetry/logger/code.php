<?php

declare(strict_types=1);

use function Flow\Telemetry\DSL\{
    console_exporter,
    logger_provider,
    memory_context_storage,
    memory_log_processor,
    resource_detector,
    telemetry
};
use function Flow\ETL\DSL\clock;

require __DIR__ . '/vendor/autoload.php';

$telemetry = telemetry(
    resource_detector()->detect(),
    null,
    null,
    logger_provider(
        memory_log_processor(console_exporter(colors: false, maxLogBodyLength: 200)),
        clock(),
        memory_context_storage(),
    ),
)->registerShutdownFunction();

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
