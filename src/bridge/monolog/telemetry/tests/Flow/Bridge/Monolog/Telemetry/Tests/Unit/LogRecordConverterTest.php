<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Bridge\Monolog\Telemetry\LogRecordConverter;
use Flow\Bridge\Monolog\Telemetry\SeverityMapper;
use Flow\Bridge\Monolog\Telemetry\Tests\Fixtures\InterpolationBackedEnumFixture;
use Flow\Bridge\Monolog\Telemetry\Tests\Fixtures\InterpolationUnitEnumFixture;
use Flow\Bridge\Monolog\Telemetry\ValueNormalizer;
use Flow\Telemetry\Logger\Severity;
use Generator;
use Monolog\Level;
use Monolog\LogRecord;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;

#[CoversClass(LogRecordConverter::class)]
final class LogRecordConverterTest extends TestCase
{
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

    public function test_accepts_custom_severity_mapper(): void
    {
        $customMapper = new SeverityMapper([
            Level::Debug->value => Severity::TRACE,
        ]);
        $converter = new LogRecordConverter($customMapper);

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Debug,
            message: 'Debug message',
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame(Severity::TRACE, $telemetryRecord->severity);
    }

    public function test_accepts_custom_value_normalizer(): void
    {
        $normalizer = new ValueNormalizer();
        $converter = new LogRecordConverter(valueNormalizer: $normalizer);

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'nullable' => null,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('null', $telemetryRecord->attributes->get('context.nullable'));
    }

    public function test_converts_basic_message(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test-channel',
            level: Level::Info,
            message: 'Hello World',
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('Hello World', $telemetryRecord->body);
        static::assertSame(Severity::INFO, $telemetryRecord->severity);
    }

    public function test_converts_context_with_prefix(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'User action',
            context: [
                'user_id' => 123,
                'action' => 'login',
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame(123, $telemetryRecord->attributes->get('context.user_id'));
        static::assertSame('login', $telemetryRecord->attributes->get('context.action'));
    }

    public function test_converts_extra_with_prefix(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Request processed',
            extra: [
                'request_id' => 'abc-123',
                'duration_ms' => 150,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('abc-123', $telemetryRecord->attributes->get('extra.request_id'));
        static::assertSame(150, $telemetryRecord->attributes->get('extra.duration_ms'));
    }

    public function test_handles_mixed_context_and_extra(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
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

        static::assertSame('Something happened', $telemetryRecord->body);
        static::assertSame(Severity::WARN, $telemetryRecord->severity);
        static::assertSame('app', $telemetryRecord->attributes->get('monolog.channel'));
        static::assertSame('Warning', $telemetryRecord->attributes->get('monolog.level_name'));
        static::assertSame(42, $telemetryRecord->attributes->get('context.user_id'));
        static::assertSame('xyz-789', $telemetryRecord->attributes->get('extra.trace_id'));
    }

    public function test_handles_nested_arrays_in_context(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
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
        static::assertIsArray($nested);
        static::assertSame('value1', $nested['key1']);
        static::assertSame(123, $nested['key2']);
    }

    public function test_handles_throwable_in_context_with_set_exception(): void
    {
        $converter = new LogRecordConverter();
        $exception = new RuntimeException('Something went wrong');

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Error,
            message: 'Error occurred',
            context: [
                'exception' => $exception,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame(RuntimeException::class, $telemetryRecord->attributes->get('exception.type'));
        static::assertSame('Something went wrong', $telemetryRecord->attributes->get('exception.message'));
        static::assertNotNull($telemetryRecord->attributes->get('exception.stacktrace'));
    }

    public function test_interpolates_message_placeholders_from_context(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'User {user_id} performed {action}',
            context: [
                'user_id' => 123,
                'action' => 'login',
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('User 123 performed login', $telemetryRecord->body);
        static::assertSame(123, $telemetryRecord->attributes->get('context.user_id'));
        static::assertSame('login', $telemetryRecord->attributes->get('context.action'));
    }

    public function test_interpolation_leaves_absent_placeholders_intact(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'User {user_id} did {missing}',
            context: [
                'user_id' => 123,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('User 123 did {missing}', $telemetryRecord->body);
    }

    public function test_interpolation_renders_array_as_json(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Tags {tags}',
            context: [
                'tags' => ['a', 'b'],
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('Tags array["a","b"]', $telemetryRecord->body);
    }

    public function test_interpolation_renders_datetime(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'At {when}',
            context: [
                'when' => new DateTimeImmutable('2024-06-15 14:30:00', new DateTimeZone('UTC')),
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('At 2024-06-15T14:30:00.000000+00:00', $telemetryRecord->body);
    }

    public function test_interpolation_renders_enums(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'backed {backed} pure {pure}',
            context: [
                'backed' => InterpolationBackedEnumFixture::Active,
                'pure' => InterpolationUnitEnumFixture::First,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('backed active pure First', $telemetryRecord->body);
    }

    public function test_interpolation_renders_plain_object_as_object_tag(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'obj {obj}',
            context: [
                'obj' => new stdClass(),
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('obj [object stdClass]', $telemetryRecord->body);
    }

    public function test_interpolation_renders_null_as_empty_string(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'val={item}',
            context: [
                'item' => null,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('val=', $telemetryRecord->body);
    }

    public function test_interpolation_renders_resource_with_type_fallback(): void
    {
        $converter = new LogRecordConverter();
        $resource = fopen('php://memory', 'rb');
        static::assertIsResource($resource);

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'res={res}',
            context: [
                'res' => $resource,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('res=[resource]', $telemetryRecord->body);

        fclose($resource);
    }

    public function test_interpolation_uses_stringable_objects(): void
    {
        $converter = new LogRecordConverter();
        $stringable = new class {
            public function __toString(): string
            {
                return 'CTX';
            }
        };

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'value={item}',
            context: [
                'item' => $stringable,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('value=CTX', $telemetryRecord->body);
    }

    public function test_message_without_braces_is_returned_verbatim(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'no placeholders here',
            context: [
                'user_id' => 5,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('no placeholders here', $telemetryRecord->body);
    }

    public function test_includes_channel_attribute(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'my-channel',
            level: Level::Info,
            message: 'Test',
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('my-channel', $telemetryRecord->attributes->get('monolog.channel'));
    }

    public function test_includes_level_name_attribute(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Warning,
            message: 'Test',
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('Warning', $telemetryRecord->attributes->get('monolog.level_name'));
    }

    #[DataProvider('levelToSeverityProvider')]
    public function test_maps_all_monolog_levels(Level $level, Severity $expectedSeverity): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: $level,
            message: 'Test message',
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame($expectedSeverity, $telemetryRecord->severity);
    }

    public function test_normalizes_datetime_in_context(): void
    {
        $converter = new LogRecordConverter();
        $datetime = new DateTimeImmutable('2024-01-15 10:30:00');

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'timestamp' => $datetime,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame($datetime, $telemetryRecord->attributes->get('context.timestamp'));
    }

    public function test_normalizes_objects_in_context(): void
    {
        $converter = new LogRecordConverter();
        $object = new class {
            public function __toString(): string
            {
                return 'custom-string';
            }
        };

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'object' => $object,
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('custom-string', $telemetryRecord->attributes->get('context.object'));
    }

    public function test_normalizes_stdclass_to_class_name(): void
    {
        $converter = new LogRecordConverter();

        $record = new LogRecord(
            datetime: new DateTimeImmutable(),
            channel: 'test',
            level: Level::Info,
            message: 'Test',
            context: [
                'object' => new stdClass(),
            ],
        );

        $telemetryRecord = $converter->convert($record);

        static::assertSame('stdClass', $telemetryRecord->attributes->get('context.object'));
    }
}
