<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use DateTimeImmutable;
use Flow\Bridge\Monolog\Telemetry\LogRecordConverter;
use Flow\Bridge\Monolog\Telemetry\SeverityMapper;
use Flow\Bridge\Monolog\Telemetry\TelemetryHandler;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Generator;
use Monolog\Level;
use Monolog\Logger as MonologLogger;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;

#[CoversClass(TelemetryHandler::class)]
final class TelemetryHandlerTest extends TestCase
{
    private MonologLogger $monolog;

    private MemoryLogProcessor $processor;

    /**
     * @return \Generator<string, array{Level, Severity}>
     */
    public static function levelToSeverityProvider(): Generator
    {
        yield 'debug' => [Level::Debug, Severity::DEBUG];
        yield 'info' => [Level::Info, Severity::INFO];
        yield 'notice' => [Level::Notice, Severity::INFO];
        yield 'warning' => [Level::Warning, Severity::WARN];
        yield 'error' => [Level::Error, Severity::ERROR];
        yield 'critical' => [Level::Critical, Severity::FATAL];
        yield 'alert' => [Level::Alert, Severity::FATAL];
        yield 'emergency' => [Level::Emergency, Severity::FATAL];
    }

    protected function setUp(): void
    {
        $this->processor = new MemoryLogProcessor(new VoidExporter());

        $loggerProvider = new LoggerProvider($this->processor, new SystemClock(), new MemoryContextStorage());

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');

        $handler = new TelemetryHandler($logger);

        $this->monolog = new MonologLogger('test-channel');
        $this->monolog->pushHandler($handler);
    }

    public function test_handler_accepts_custom_converter(): void
    {
        $processor = new MemoryLogProcessor(new VoidExporter());

        $loggerProvider = new LoggerProvider($processor, new SystemClock(), new MemoryContextStorage());

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');

        $customMapper = new SeverityMapper([
            Level::Debug->value => Severity::TRACE,
            Level::Info->value => Severity::INFO,
            Level::Notice->value => Severity::INFO,
            Level::Warning->value => Severity::WARN,
            Level::Error->value => Severity::ERROR,
            Level::Critical->value => Severity::FATAL,
            Level::Alert->value => Severity::FATAL,
            Level::Emergency->value => Severity::FATAL,
        ]);

        $converter = new LogRecordConverter($customMapper);
        $handler = new TelemetryHandler($logger, $converter);

        $monolog = new MonologLogger('test-channel');
        $monolog->pushHandler($handler);

        $monolog->debug('Debug message');

        static::assertCount(1, $processor->entries());
        static::assertSame(Severity::TRACE, $processor->entries()[0]->record->severity);
    }

    public function test_handler_converts_context_to_prefixed_attributes(): void
    {
        $this->monolog->info('User action', [
            'user_id' => 123,
            'action' => 'login',
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame(123, $entries[0]->record->attributes->get('context.user_id'));
        static::assertSame('login', $entries[0]->record->attributes->get('context.action'));
    }

    public function test_handler_converts_extra_to_prefixed_attributes(): void
    {
        $this->monolog->pushProcessor(static function ($record) {
            $record->extra['request_id'] = 'abc-123';

            return $record;
        });

        $this->monolog->info('Request processed');

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('abc-123', $entries[0]->record->attributes->get('extra.request_id'));
    }

    public function test_handler_interpolates_message_placeholders_from_context(): void
    {
        $this->monolog->info('User {user_id} performed {action}', [
            'user_id' => 123,
            'action' => 'login',
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('User 123 performed login', $entries[0]->record->body);
        static::assertSame(123, $entries[0]->record->attributes->get('context.user_id'));
        static::assertSame('login', $entries[0]->record->attributes->get('context.action'));
    }

    public function test_handler_forwards_message_body(): void
    {
        $this->monolog->info('Hello World');

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('Hello World', $entries[0]->record->body);
    }

    public function test_handler_handles_exception_in_context(): void
    {
        $exception = new RuntimeException('Something went wrong');

        $this->monolog->error('Error occurred', [
            'exception' => $exception,
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame(RuntimeException::class, $entries[0]->record->attributes->get('exception.type'));
        static::assertSame('Something went wrong', $entries[0]->record->attributes->get('exception.message'));
        static::assertNotNull($entries[0]->record->attributes->get('exception.stacktrace'));
    }

    public function test_handler_handles_nested_arrays_in_context(): void
    {
        $this->monolog->info('Test', [
            'nested' => [
                'key1' => 'value1',
                'key2' => 123,
            ],
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);

        $nested = $entries[0]->record->attributes->get('context.nested');
        static::assertIsArray($nested);
        static::assertSame('value1', $nested['key1']);
        static::assertSame(123, $nested['key2']);
    }

    public function test_handler_includes_channel_as_attribute(): void
    {
        $this->monolog->info('Test');

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('test-channel', $entries[0]->record->attributes->get('monolog.channel'));
    }

    public function test_handler_includes_level_name_as_attribute(): void
    {
        $this->monolog->warning('Test');

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('Warning', $entries[0]->record->attributes->get('monolog.level_name'));
    }

    #[DataProvider('levelToSeverityProvider')]
    public function test_handler_maps_monolog_level_to_telemetry_severity(
        Level $level,
        Severity $expectedSeverity,
    ): void {
        $this->monolog->log($level, 'Test message');

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame($expectedSeverity, $entries[0]->record->severity);
    }

    public function test_handler_normalizes_datetime_values(): void
    {
        $datetime = new DateTimeImmutable('2024-01-15 10:30:00');

        $this->monolog->info('Test', [
            'timestamp' => $datetime,
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame($datetime, $entries[0]->record->attributes->get('context.timestamp'));
    }

    public function test_handler_normalizes_null_values(): void
    {
        $this->monolog->info('Test', [
            'nullable' => null,
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('null', $entries[0]->record->attributes->get('context.nullable'));
    }

    public function test_handler_normalizes_objects_with_to_string(): void
    {
        $object = new class {
            public function __toString(): string
            {
                return 'custom-string';
            }
        };

        $this->monolog->info('Test', [
            'object' => $object,
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('custom-string', $entries[0]->record->attributes->get('context.object'));
    }

    public function test_handler_normalizes_objects_without_to_string_to_class_name(): void
    {
        $object = new stdClass();

        $this->monolog->info('Test', [
            'object' => $object,
        ]);

        $entries = $this->processor->entries();
        static::assertCount(1, $entries);
        static::assertSame('stdClass', $entries[0]->record->attributes->get('context.object'));
    }

    public function test_handler_respects_minimum_level(): void
    {
        $processor = new MemoryLogProcessor(new VoidExporter());

        $loggerProvider = new LoggerProvider($processor, new SystemClock(), new MemoryContextStorage());

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');

        $handler = new TelemetryHandler($logger, level: Level::Warning);

        $monolog = new MonologLogger('test-channel');
        $monolog->pushHandler($handler);

        $monolog->debug('Debug message');
        $monolog->info('Info message');
        $monolog->warning('Warning message');
        $monolog->error('Error message');

        static::assertCount(2, $processor->entries());
        static::assertSame('Warning message', $processor->entries()[0]->record->body);
        static::assertSame('Error message', $processor->entries()[1]->record->body);
    }
}
