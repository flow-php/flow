<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Telemetry\ParameterFormatter;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class ParameterFormatterTest extends TestCase
{
    public function test_convert_to_string_array() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('[1,2,3]', $formatter->convertToString([1, 2, 3]));
    }

    public function test_convert_to_string_associative_array() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('{"name":"John","age":30}', $formatter->convertToString(['name' => 'John', 'age' => 30]));
    }

    public function test_convert_to_string_boolean_false() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('false', $formatter->convertToString(false));
    }

    public function test_convert_to_string_boolean_true() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('true', $formatter->convertToString(true));
    }

    public function test_convert_to_string_datetime() : void
    {
        $formatter = new ParameterFormatter();
        $date = new \DateTimeImmutable('2024-01-15T10:30:00+00:00');

        self::assertSame('2024-01-15T10:30:00+00:00', $formatter->convertToString($date));
    }

    public function test_convert_to_string_float() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('3.14', $formatter->convertToString(3.14));
    }

    public function test_convert_to_string_integer() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('42', $formatter->convertToString(42));
    }

    public function test_convert_to_string_null() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('NULL', $formatter->convertToString(null));
    }

    public function test_convert_to_string_object_with_to_string() : void
    {
        $formatter = new ParameterFormatter();
        $object = new class {
            public function __toString() : string
            {
                return 'custom_string';
            }
        };

        self::assertSame('custom_string', $formatter->convertToString($object));
    }

    public function test_convert_to_string_object_without_to_string() : void
    {
        $formatter = new ParameterFormatter();
        $object = new \stdClass();

        self::assertSame('stdClass', $formatter->convertToString($object));
    }

    public function test_convert_to_string_resource() : void
    {
        $formatter = new ParameterFormatter();
        $resource = \fopen('php://memory', 'rb');

        self::assertSame('resource (stream)', $formatter->convertToString($resource));

        \fclose($resource);
    }

    public function test_convert_to_string_string() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('hello world', $formatter->convertToString('hello world'));
    }

    public function test_convert_to_string_typed_value() : void
    {
        $formatter = new ParameterFormatter();
        $typedValue = new TypedValue('some_value', PostgreSqlType::TEXT);

        $result = $formatter->convertToString($typedValue);

        self::assertSame('{"type":"TEXT","value":"some_value"}', $result);
    }

    public function test_convert_to_string_typed_value_with_nested_value() : void
    {
        $formatter = new ParameterFormatter();
        $typedValue = new TypedValue(['a', 'b', 'c'], PostgreSqlType::TEXT_ARRAY);

        $result = $formatter->convertToString($typedValue);

        self::assertSame('{"type":"TEXT_ARRAY","value":"[\"a\",\"b\",\"c\"]"}', $result);
    }

    public function test_format_does_not_truncate_when_max_length_null() : void
    {
        $formatter = new ParameterFormatter();
        $longValue = \str_repeat('a', 200);

        self::assertSame($longValue, $formatter->format($longValue, null));
    }

    public function test_format_does_not_truncate_when_under_max_length() : void
    {
        $formatter = new ParameterFormatter();

        self::assertSame('short', $formatter->format('short', 100));
    }

    public function test_format_truncates_at_exact_boundary() : void
    {
        $formatter = new ParameterFormatter();
        $value = 'exactly10!';

        self::assertSame('exactly10!', $formatter->format($value, 10));
    }

    public function test_format_truncates_when_over_max_length() : void
    {
        $formatter = new ParameterFormatter();
        $longValue = \str_repeat('a', 200);

        $result = $formatter->format($longValue, 100);

        self::assertSame(103, \strlen($result));
        self::assertStringEndsWith('...', $result);
        self::assertStringStartsWith(\str_repeat('a', 100), $result);
    }
}
