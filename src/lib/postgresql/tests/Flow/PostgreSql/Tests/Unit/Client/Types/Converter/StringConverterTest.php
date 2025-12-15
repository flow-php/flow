<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\StringConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class StringConverterTest extends TestCase
{
    public function test_empty_string() : void
    {
        $converter = new StringConverter();
        self::assertSame('', $converter->toDatabase(''));
        self::assertSame('', $converter->toPhp('', PostgreSqlType::TEXT));
    }

    public function test_non_stringable_returns_empty_string() : void
    {
        $converter = new StringConverter();
        self::assertSame('', $converter->toDatabase(['array']));
    }

    public function test_null_handling() : void
    {
        $converter = new StringConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new StringConverter();
        $value = 'hello world';

        $dbValue = $converter->toDatabase($value);
        self::assertNotNull($dbValue);
        self::assertSame('hello world', $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::TEXT);
        self::assertSame($value, $phpValue);
    }

    public function test_scalar_conversion() : void
    {
        $converter = new StringConverter();
        self::assertSame('42', $converter->toDatabase(42));
        self::assertSame('3.14', $converter->toDatabase(3.14));
        self::assertSame('1', $converter->toDatabase(true));
    }

    public function test_stringable_object_conversion() : void
    {
        $converter = new StringConverter();
        $stringable = new class {
            public function __toString() : string
            {
                return 'stringable';
            }
        };
        self::assertSame('stringable', $converter->toDatabase($stringable));
    }

    public function test_supported_types() : void
    {
        $converter = new StringConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::TEXT, $types);
        self::assertContains(PostgreSqlType::VARCHAR, $types);
        self::assertContains(PostgreSqlType::CHAR, $types);
        self::assertContains(PostgreSqlType::BPCHAR, $types);
    }
}
