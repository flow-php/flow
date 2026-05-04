<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Unit;

use Flow\Bridge\Psr3\Telemetry\Exception\InvalidArgumentException;
use Flow\Bridge\Psr3\Telemetry\{LogRecordConverter, SeverityMapper, ValueNormalizer};
use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

final class LogRecordConverterTest extends TestCase
{
    public function test_accepts_stringable_message() : void
    {
        $message = new class implements \Stringable {
            public function __toString() : string
            {
                return 'rendered body';
            }
        };

        $record = (new LogRecordConverter())->convert(LogLevel::INFO, $message);

        self::assertSame('rendered body', $record->body);
    }

    public function test_context_entries_become_attributes_with_raw_keys() : void
    {
        $record = (new LogRecordConverter())->convert(LogLevel::INFO, 'msg', [
            'user_id' => 123,
            'role' => 'admin',
        ]);

        self::assertSame(123, $record->attributes->get('user_id'));
        self::assertSame('admin', $record->attributes->get('role'));
    }

    public function test_default_normalizer_handles_null_in_attributes() : void
    {
        $record = (new LogRecordConverter(valueNormalizer: new ValueNormalizer()))
            ->convert(LogLevel::INFO, 'msg', ['nullable' => null]);

        self::assertSame('null', $record->attributes->get('nullable'));
    }

    public function test_exception_in_context_is_routed_via_set_exception() : void
    {
        $exception = new \RuntimeException('boom');

        $record = (new LogRecordConverter())->convert(LogLevel::ERROR, 'failure', [
            'exception' => $exception,
        ]);

        self::assertSame(\RuntimeException::class, $record->attributes->get('exception.type'));
        self::assertSame('boom', $record->attributes->get('exception.message'));
        self::assertNotNull($record->attributes->get('exception.stacktrace'));
        self::assertFalse($record->attributes->has('exception'));
    }

    public function test_interpolates_message_placeholders_from_context() : void
    {
        $record = (new LogRecordConverter())->convert(LogLevel::INFO, 'User {user_id} performed {action}', [
            'user_id' => 123,
            'action' => 'login',
        ]);

        self::assertSame('User 123 performed login', $record->body);
        self::assertSame(123, $record->attributes->get('user_id'));
        self::assertSame('login', $record->attributes->get('action'));
    }

    public function test_interpolation_skips_arrays_and_throwables_and_plain_objects() : void
    {
        $record = (new LogRecordConverter())->convert(
            LogLevel::INFO,
            'Tags {tags} error {exception} obj {obj}',
            [
                'tags' => ['a', 'b'],
                'exception' => new \RuntimeException('x'),
                'obj' => new \stdClass(),
            ],
        );

        self::assertSame('Tags {tags} error {exception} obj {obj}', $record->body);
    }

    public function test_interpolation_uses_stringable_objects() : void
    {
        $stringable = new class implements \Stringable {
            public function __toString() : string
            {
                return 'CTX';
            }
        };

        $record = (new LogRecordConverter())->convert(LogLevel::INFO, 'value={item}', [
            'item' => $stringable,
        ]);

        self::assertSame('value=CTX', $record->body);
    }

    public function test_message_without_braces_is_returned_verbatim() : void
    {
        $record = (new LogRecordConverter())->convert(LogLevel::INFO, 'no placeholders here', [
            'user_id' => 5,
        ]);

        self::assertSame('no placeholders here', $record->body);
    }

    public function test_non_throwable_under_exception_key_is_stored_as_attribute() : void
    {
        $record = (new LogRecordConverter())->convert(LogLevel::ERROR, 'msg', [
            'exception' => 'not-a-throwable',
        ]);

        self::assertSame('not-a-throwable', $record->attributes->get('exception'));
        self::assertFalse($record->attributes->has('exception.type'));
    }

    public function test_normalizes_arbitrary_php_values_in_context() : void
    {
        $record = (new LogRecordConverter())->convert(LogLevel::INFO, 'msg', [
            'nullable' => null,
            'object' => new \stdClass(),
        ]);

        self::assertSame('null', $record->attributes->get('nullable'));
        self::assertSame(\stdClass::class, $record->attributes->get('object'));
    }

    public function test_severity_mapping_uses_provided_mapper() : void
    {
        $mapper = new SeverityMapper([
            LogLevel::DEBUG => Severity::TRACE,
            LogLevel::INFO => Severity::INFO,
            LogLevel::NOTICE => Severity::INFO,
            LogLevel::WARNING => Severity::WARN,
            LogLevel::ERROR => Severity::ERROR,
            LogLevel::CRITICAL => Severity::FATAL,
            LogLevel::ALERT => Severity::FATAL,
            LogLevel::EMERGENCY => Severity::FATAL,
        ]);

        $record = (new LogRecordConverter($mapper))->convert(LogLevel::DEBUG, 'msg');

        self::assertSame(Severity::TRACE, $record->severity);
    }

    public function test_throws_on_unknown_level() : void
    {
        $this->expectException(InvalidArgumentException::class);

        (new LogRecordConverter())->convert('verbose', 'msg');
    }
}
