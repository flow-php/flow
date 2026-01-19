<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use Flow\Bridge\Monolog\Telemetry\{LogRecordConverter, SeverityMapper, ValueNormalizer};
use Flow\Telemetry\Logger\Severity;
use Monolog\{Level, LogRecord};
use PHPUnit\Framework\Attributes\{CoversClass, DataProvider};
use PHPUnit\Framework\TestCase;

#[CoversClass(LogRecordConverter::class)]
final class LogRecordConverterTest extends TestCase
{
    /**
     * @return \Generator<string, array{Level, Severity}>
     */
    public static function levelToSeverityProvider() : \Generator
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

    public function test_accepts_custom_severity_mapper() : void
    {
        $customMapper = new SeverityMapper([
            Level::Debug->value => Severity::TRACE,
        ]);
        $converter = new LogRecordConverter($customMapper);

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Debug,
            message: 'Debug message',
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame(Severity::TRACE, $telemetryRecord->severity);
    }

    public function test_accepts_custom_value_normalizer() : void
    {
        $normalizer = new ValueNormalizer();
        $converter = new LogRecordConverter(valueNormalizer: $normalizer);

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'nullable' => null,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('null', $telemetryRecord->attributes->get('context.nullable'));
    }

    public function test_converts_basic_message() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test-channel',
            level: Level::Info,
            message: 'Hello World',
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('Hello World', $telemetryRecord->body);
        self::assertSame(Severity::INFO, $telemetryRecord->severity);
    }

    public function test_converts_context_with_prefix() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'User action',
            context: [
                'user_id' => 123,
                'action' => 'login',
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame(123, $telemetryRecord->attributes->get('context.user_id'));
        self::assertSame('login', $telemetryRecord->attributes->get('context.action'));
    }

    public function test_converts_extra_with_prefix() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Request processed',
            extra: [
                'request_id' => 'abc-123',
                'duration_ms' => 150,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('abc-123', $telemetryRecord->attributes->get('extra.request_id'));
        self::assertSame(150, $telemetryRecord->attributes->get('extra.duration_ms'));
    }

    public function test_handles_mixed_context_and_extra() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'app',
            level: Level::Warning,
            message: 'Something happened',
            context: [
                'user_id' => 42,
            ],
            extra: [
                'trace_id' => 'xyz-789',
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('Something happened', $telemetryRecord->body);
        self::assertSame(Severity::WARN, $telemetryRecord->severity);
        self::assertSame('app', $telemetryRecord->attributes->get('monolog.channel'));
        self::assertSame('Warning', $telemetryRecord->attributes->get('monolog.level_name'));
        self::assertSame(42, $telemetryRecord->attributes->get('context.user_id'));
        self::assertSame('xyz-789', $telemetryRecord->attributes->get('extra.trace_id'));
    }

    public function test_handles_nested_arrays_in_context() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'nested' => [
                    'key1' => 'value1',
                    'key2' => 123,
                ],
            ],
        );

        $telemetryRecord = $converter->convert($record);

        $nested = $telemetryRecord->attributes->get('context.nested');
        self::assertIsArray($nested);
        self::assertSame('value1', $nested['key1']);
        self::assertSame(123, $nested['key2']);
    }

    public function test_handles_throwable_in_context_with_set_exception() : void
    {
        $converter = new LogRecordConverter();
        $exception = new \RuntimeException('Something went wrong');

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Error,
            message: 'Error occurred',
            context: [
                'exception' => $exception,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame(\RuntimeException::class, $telemetryRecord->attributes->get('exception.type'));
        self::assertSame('Something went wrong', $telemetryRecord->attributes->get('exception.message'));
        self::assertNotNull($telemetryRecord->attributes->get('exception.stacktrace'));
    }

    public function test_includes_channel_attribute() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'my-channel',
            level: Level::Info,
            message: 'Test',
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('my-channel', $telemetryRecord->attributes->get('monolog.channel'));
    }

    public function test_includes_level_name_attribute() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Warning,
            message: 'Test',
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('Warning', $telemetryRecord->attributes->get('monolog.level_name'));
    }

    #[DataProvider('levelToSeverityProvider')]
    public function test_maps_all_monolog_levels(Level $level, Severity $expectedSeverity) : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: $level,
            message: 'Test message',
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame($expectedSeverity, $telemetryRecord->severity);
    }

    public function test_normalizes_datetime_in_context() : void
    {
        $converter = new LogRecordConverter();
        $datetime = new \DateTimeImmutable('2024-01-15 10:30:00');

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'timestamp' => $datetime,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame($datetime, $telemetryRecord->attributes->get('context.timestamp'));
    }

    public function test_normalizes_objects_in_context() : void
    {
        $converter = new LogRecordConverter();
        $object = new class {
            public function __toString() : string
            {
                return 'custom-string';
            }
        };

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'object' => $object,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('custom-string', $telemetryRecord->attributes->get('context.object'));
    }

    public function test_normalizes_stdclass_to_class_name() : void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new \DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'object' => new \stdClass(),
            ],
        );

        $telemetryRecord = $converter->convert($record);

        self::assertSame('stdClass', $telemetryRecord->attributes->get('context.object'));
    }
}
