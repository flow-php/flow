<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\ByteaConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class ByteaConverterTest extends TestCase
{
    public function test_empty_string() : void
    {
        $converter = new ByteaConverter();
        self::assertSame('', $converter->toDatabase(''));
        self::assertSame('', $converter->toPhp('\\x', PostgreSqlType::BYTEA));
    }

    public function test_non_string_returns_empty() : void
    {
        $converter = new ByteaConverter();
        self::assertSame('', $converter->toDatabase(12345));
        self::assertSame('', $converter->toDatabase(['array']));
    }

    public function test_null_handling() : void
    {
        $converter = new ByteaConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new ByteaConverter();
        $text = 'simple text';

        $dbValue = $converter->toDatabase($text);
        self::assertNotNull($dbValue);
        self::assertSame($text, $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::BYTEA);
        self::assertSame($text, $phpValue);
    }

    public function test_supported_types() : void
    {
        $converter = new ByteaConverter();
        self::assertContains(PostgreSqlType::BYTEA, $converter->supportedTypes());
    }

    public function test_to_database_format() : void
    {
        $converter = new ByteaConverter();
        $text = 'hello';

        $dbValue = $converter->toDatabase($text);
        self::assertSame('hello', $dbValue);
    }

    public function test_to_php_with_escape_format() : void
    {
        $converter = new ByteaConverter();
        $result = $converter->toPhp('hello', PostgreSqlType::BYTEA);
        self::assertSame('hello', $result);
    }

    public function test_to_php_with_hex_format() : void
    {
        $converter = new ByteaConverter();
        $result = $converter->toPhp('\\x68656c6c6f', PostgreSqlType::BYTEA);
        self::assertSame('hello', $result);
    }

    public function test_to_php_with_odd_length_hex_returns_original() : void
    {
        $converter = new ByteaConverter();
        $result = @$converter->toPhp('\\xinvalid', PostgreSqlType::BYTEA);
        self::assertSame('\\xinvalid', $result);
    }
}
