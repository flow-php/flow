<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Integration;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Bridge\Monolog\Telemetry\LogRecordConverter;
use Flow\Bridge\Monolog\Telemetry\SeverityMapper;
use Flow\Bridge\Monolog\Telemetry\TelemetryHandler;
use Flow\Bridge\Monolog\Telemetry\ValueNormalizer;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Resource;
use InvalidArgumentException;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\Logger as MonologLogger;
use Monolog\LogRecord;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\Bridge\Monolog\Telemetry\DSL\log_record_converter;
use function Flow\Bridge\Monolog\Telemetry\DSL\severity_mapper;
use function Flow\Bridge\Monolog\Telemetry\DSL\telemetry_handler;

#[CoversClass(TelemetryHandler::class)]
#[CoversClass(LogRecordConverter::class)]
#[CoversClass(ValueNormalizer::class)]
#[CoversClass(SeverityMapper::class)]
final class TelemetryHandlerIntegrationTest extends TestCase
{
    public function test_basic_logging_flow_through_all_components(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->debug('Debug message');
        $monolog->info('Info message');
        $monolog->warning('Warning message');
        $monolog->error('Error message');

        $entries = $context->processor->entries();
        static::assertCount(4, $entries);

        static::assertSame(Severity::DEBUG, $entries[0]->record->severity);
        static::assertSame('Debug message', $entries[0]->record->body);

        static::assertSame(Severity::INFO, $entries[1]->record->severity);
        static::assertSame('Info message', $entries[1]->record->body);

        static::assertSame(Severity::WARN, $entries[2]->record->severity);
        static::assertSame('Warning message', $entries[2]->record->body);

        static::assertSame(Severity::ERROR, $entries[3]->record->severity);
        static::assertSame('Error message', $entries[3]->record->body);
    }

    public function test_context_and_extra_data_flow_through_pipeline(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));
        $monolog->pushProcessor(static function ($record) {
            $record->extra['request_id'] = 'req-abc-123';
            $record->extra['server'] = 'web-01';

            return $record;
        });

        $monolog->info('User logged in', [
            'user_id' => 42,
            'action' => 'login',
            'ip_address' => '192.168.1.1',
        ]);

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        $attributes = $entries[0]->record->attributes;
        static::assertSame(42, $attributes->get('context.user_id'));
        static::assertSame('login', $attributes->get('context.action'));
        static::assertSame('192.168.1.1', $attributes->get('context.ip_address'));
        static::assertSame('req-abc-123', $attributes->get('extra.request_id'));
        static::assertSame('web-01', $attributes->get('extra.server'));
    }

    public function test_custom_converter_integration(): void
    {
        $context = TelemetryTestContext::create();

        $customMapper = severity_mapper([
            Level::Debug->value => Severity::TRACE,
            Level::Info->value => Severity::DEBUG,
            Level::Notice->value => Severity::INFO,
            Level::Warning->value => Severity::INFO,
            Level::Error->value => Severity::WARN,
            Level::Critical->value => Severity::ERROR,
            Level::Alert->value => Severity::ERROR,
            Level::Emergency->value => Severity::FATAL,
        ]);

        $converter = log_record_converter(severityMapper: $customMapper);

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger, converter: $converter));

        $monolog->debug('Debug mapped to TRACE');
        $monolog->info('Info mapped to DEBUG');
        $monolog->error('Error mapped to WARN');

        $entries = $context->processor->entries();
        static::assertCount(3, $entries);

        static::assertSame(Severity::TRACE, $entries[0]->record->severity);
        static::assertSame(Severity::DEBUG, $entries[1]->record->severity);
        static::assertSame(Severity::WARN, $entries[2]->record->severity);
    }

    public function test_datetime_values_preserved_in_context(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $eventTime = new DateTimeImmutable('2024-06-15 14:30:00');

        $monolog->info('Event occurred', [
            'event_time' => $eventTime,
        ]);

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        $contextEventTime = $entries[0]->record->attributes->get('context.event_time');
        static::assertInstanceOf(DateTimeInterface::class, $contextEventTime);
        static::assertSame('2024-06-15 14:30:00', $contextEventTime->format('Y-m-d H:i:s'));
    }

    public function test_empty_context_and_extra_handling(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->info('Simple message without context');

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        static::assertSame('Simple message without context', $entries[0]->record->body);
        static::assertSame('application', $entries[0]->record->attributes->get('monolog.channel'));
        static::assertSame('Info', $entries[0]->record->attributes->get('monolog.level_name'));
    }

    public function test_exception_handling_flow(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $exception = new RuntimeException('Database connection failed', 500);

        $monolog->error('Failed to process request', [
            'exception' => $exception,
            'operation' => 'db_query',
        ]);

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        $attributes = $entries[0]->record->attributes;
        static::assertSame(RuntimeException::class, $attributes->get('exception.type'));
        static::assertSame('Database connection failed', $attributes->get('exception.message'));
        static::assertNotNull($attributes->get('exception.stacktrace'));
        static::assertSame('db_query', $attributes->get('context.operation'));
    }

    public function test_handler_bubble_behavior(): void
    {
        $context = TelemetryTestContext::create();

        $callCount = 0;
        $countingHandler = new class($callCount) extends AbstractProcessingHandler {
            public function __construct(
                private int &$callCount,
            ) {
                parent::__construct();
            }

            protected function write(LogRecord $record): void
            {
                $this->callCount++;
            }
        };

        $monolog = new MonologLogger('application');
        $monolog->pushHandler($countingHandler);
        $monolog->pushHandler(telemetry_handler($context->logger, bubble: true));

        $monolog->info('Message that should bubble');

        static::assertCount(1, $context->processor->entries());
        static::assertSame(1, $callCount);

        $context->processor->reset();
        $callCount = 0;

        $monolog2 = new MonologLogger('application');
        $monolog2->pushHandler($countingHandler);
        $monolog2->pushHandler(telemetry_handler($context->logger, bubble: false));

        $monolog2->info('Message that should not bubble');

        static::assertCount(1, $context->processor->entries());
        static::assertSame(0, $callCount);
    }

    public function test_logs_after_span_completion_have_no_trace_context(): void
    {
        $context = TelemetryTestContext::createWithTracing();
        static::assertNotNull($context->tracer);

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $span = $context->tracer->span('short-operation');
        $monolog->info('Inside span');
        $context->tracer->complete($span);

        $monolog->info('After span completed');

        $entries = $context->processor->entries();
        static::assertCount(2, $entries);

        static::assertNotNull($entries[0]->spanContext, 'Log inside span should have context');
        static::assertNull($entries[1]->spanContext, 'Log after span should not have context');
    }

    public function test_logs_within_active_span_include_trace_and_span_ids(): void
    {
        $context = TelemetryTestContext::createWithTracing();
        static::assertNotNull($context->tracer);

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $span = $context->tracer->span('process-order');

        $monolog->info('Processing order', ['order_id' => 123]);
        $monolog->warning('Low inventory', ['product_id' => 456]);

        $context->tracer->complete($span);

        $entries = $context->processor->entries();
        static::assertCount(2, $entries);

        static::assertNotNull($entries[0]->spanContext);
        static::assertNotNull($entries[0]->spanContext->traceId);
        static::assertNotNull($entries[0]->spanContext->spanId);

        static::assertNotNull($entries[1]->spanContext);
        static::assertSame(
            $entries[0]->spanContext->traceId->toHex(),
            $entries[1]->spanContext->traceId->toHex(),
            'Both logs should share the same trace_id',
        );
        static::assertSame(
            $entries[0]->spanContext->spanId->toHex(),
            $entries[1]->spanContext->spanId->toHex(),
            'Both logs should share the same span_id',
        );
    }

    public function test_logs_without_active_span_have_no_trace_context(): void
    {
        $context = TelemetryTestContext::createWithTracing();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->info('Log without span context');

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        static::assertNull($entries[0]->spanContext);
    }

    public function test_minimum_level_filtering(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger, level: Level::Warning));

        $monolog->debug('This should be filtered');
        $monolog->info('This should be filtered too');
        $monolog->warning('This should pass');
        $monolog->error('This should also pass');
        $monolog->critical('This should pass as well');

        $entries = $context->processor->entries();
        static::assertCount(3, $entries);

        static::assertSame('This should pass', $entries[0]->record->body);
        static::assertSame(Severity::WARN, $entries[0]->record->severity);

        static::assertSame('This should also pass', $entries[1]->record->body);
        static::assertSame(Severity::ERROR, $entries[1]->record->severity);

        static::assertSame('This should pass as well', $entries[2]->record->body);
        static::assertSame(Severity::FATAL, $entries[2]->record->severity);
    }

    public function test_multiple_channels_sharing_same_telemetry_logger(): void
    {
        $context = TelemetryTestContext::create(resource: Resource::create([
            'service.name' => 'multi-channel-service',
        ]), scope: 'shared-logger');

        $apiLogger = new MonologLogger('api');
        $apiLogger->pushHandler(telemetry_handler($context->logger));

        $dbLogger = new MonologLogger('database');
        $dbLogger->pushHandler(telemetry_handler($context->logger));

        $cacheLogger = new MonologLogger('cache');
        $cacheLogger->pushHandler(telemetry_handler($context->logger));

        $apiLogger->info('API request received');
        $dbLogger->warning('Slow query detected');
        $cacheLogger->debug('Cache miss');

        $entries = $context->processor->entries();
        static::assertCount(3, $entries);

        static::assertSame('api', $entries[0]->record->attributes->get('monolog.channel'));
        static::assertSame('API request received', $entries[0]->record->body);

        static::assertSame('database', $entries[1]->record->attributes->get('monolog.channel'));
        static::assertSame('Slow query detected', $entries[1]->record->body);

        static::assertSame('cache', $entries[2]->record->attributes->get('monolog.channel'));
        static::assertSame('Cache miss', $entries[2]->record->body);
    }

    public function test_multiple_exceptions_last_one_wins(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $primaryException = new RuntimeException('Primary error');
        $secondaryException = new InvalidArgumentException('Secondary error');

        $monolog->error('Multiple errors', [
            'exception' => $primaryException,
            'secondary_exception' => $secondaryException,
        ]);

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        $attributes = $entries[0]->record->attributes;
        static::assertSame(InvalidArgumentException::class, $attributes->get('exception.type'));
        static::assertSame('Secondary error', $attributes->get('exception.message'));
        static::assertNull($attributes->get('context.exception'));
        static::assertNull($attributes->get('context.secondary_exception'));
    }

    public function test_nested_data_structures_in_context(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->info('Complex data logged', [
            'user' => [
                'id' => 123,
                'name' => 'John Doe',
                'roles' => ['admin', 'user'],
                'metadata' => [
                    'created_at' => '2024-01-15',
                    'last_login' => '2024-01-20',
                ],
            ],
            'request' => [
                'method' => 'POST',
                'path' => '/api/users',
                'headers' => [
                    'content-type' => 'application/json',
                    'accept' => 'application/json',
                ],
            ],
        ]);

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        $userAttr = $entries[0]->record->attributes->get('context.user');
        static::assertIsArray($userAttr);
        /** @var array{id: int, name: string, roles: array<string>, metadata: array{created_at: string}} $userAttr */
        static::assertSame(123, $userAttr['id']);
        static::assertSame('John Doe', $userAttr['name']);
        static::assertSame(['admin', 'user'], $userAttr['roles']);
        static::assertSame('2024-01-15', $userAttr['metadata']['created_at']);

        $requestAttr = $entries[0]->record->attributes->get('context.request');
        static::assertIsArray($requestAttr);
        /** @var array{method: string, path: string} $requestAttr */
        static::assertSame('POST', $requestAttr['method']);
        static::assertSame('/api/users', $requestAttr['path']);
    }

    public function test_nested_spans_propagate_child_span_id_to_logs(): void
    {
        $context = TelemetryTestContext::createWithTracing();
        static::assertNotNull($context->tracer);

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $parentSpan = $context->tracer->span('parent-operation');
        $monolog->info('In parent span');

        $childSpan = $context->tracer->span('child-operation');
        $monolog->info('In child span');

        $context->tracer->complete($childSpan);
        $monolog->info('Back in parent span');

        $context->tracer->complete($parentSpan);

        $entries = $context->processor->entries();
        static::assertCount(3, $entries);

        $parentSpanId = $entries[0]->spanContext?->spanId->toHex();
        $childSpanId = $entries[1]->spanContext?->spanId->toHex();
        $backInParentSpanId = $entries[2]->spanContext?->spanId->toHex();

        static::assertNotNull($parentSpanId);
        static::assertNotNull($childSpanId);
        static::assertNotNull($backInParentSpanId);

        static::assertNotSame($parentSpanId, $childSpanId, 'Child span should have different span_id');
        static::assertSame(
            $parentSpanId,
            $backInParentSpanId,
            'After child completes, should be back to parent span_id',
        );

        static::assertSame(
            $entries[0]->spanContext->traceId->toHex(),
            $entries[1]->spanContext->traceId->toHex(),
            'All logs should share the same trace_id',
        );
        static::assertSame(
            $entries[1]->spanContext->traceId->toHex(),
            $entries[2]->spanContext->traceId->toHex(),
            'All logs should share the same trace_id',
        );
    }

    public function test_processor_filtering_by_body_content(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->info('User logged in');
        $monolog->info('User logged out');
        $monolog->error('User authentication failed');
        $monolog->info('System initialized');

        $userEntries = $context->processor->entriesContaining('User');
        static::assertCount(3, $userEntries);

        $loggedEntries = $context->processor->entriesContaining('logged');
        static::assertCount(2, $loggedEntries);
    }

    public function test_processor_filtering_by_severity(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->debug('Debug 1');
        $monolog->info('Info 1');
        $monolog->warning('Warning 1');
        $monolog->error('Error 1');
        $monolog->debug('Debug 2');
        $monolog->error('Error 2');

        $debugEntries = $context->processor->entriesWithSeverity(Severity::DEBUG);
        static::assertCount(2, $debugEntries);

        $errorEntries = $context->processor->entriesWithSeverity(Severity::ERROR);
        static::assertCount(2, $errorEntries);

        $warnEntries = $context->processor->entriesWithSeverity(Severity::WARN);
        static::assertCount(1, $warnEntries);
    }

    public function test_processor_flush_exports_all_entries(): void
    {
        $context = TelemetryTestContext::create();

        $monolog = new MonologLogger('application');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->info('Message 1');
        $monolog->info('Message 2');
        $monolog->info('Message 3');

        static::assertCount(3, $context->processor->entries());

        $result = $context->processor->flush();

        static::assertTrue($result);
    }

    public function test_resource_attributes_are_preserved(): void
    {
        $context = TelemetryTestContext::create(
            resource: Resource::create([
                'service.name' => 'my-application',
                'service.version' => '1.2.3',
                'deployment.environment' => 'production',
            ]),
            scope: 'application-logger',
            version: '2.0.0',
        );

        $monolog = new MonologLogger('app');
        $monolog->pushHandler(telemetry_handler($context->logger));

        $monolog->info('Test message');

        $entries = $context->processor->entries();
        static::assertCount(1, $entries);

        static::assertSame('my-application', $entries[0]->resource->get('service.name'));
        static::assertSame('1.2.3', $entries[0]->resource->get('service.version'));
        static::assertSame('production', $entries[0]->resource->get('deployment.environment'));
        static::assertSame('application-logger', $entries[0]->scope->name);
        static::assertSame('2.0.0', $entries[0]->scope->version);
    }
}
