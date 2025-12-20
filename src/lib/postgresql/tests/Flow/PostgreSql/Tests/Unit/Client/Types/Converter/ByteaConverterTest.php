<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\ByteaConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class ByteaConverterTest extends TestCase
{
    public function test_array_throws_exception() : void
    {
        $converter = new ByteaConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['array']);
    }

    public function test_empty_string() : void
    {
        $converter = new ByteaConverter();
        self::assertSame('', $converter->toDatabase(''));
    }

    public function test_non_string_throws_exception() : void
    {
        $converter = new ByteaConverter();
        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(12345);
    }

    public function test_null_handling() : void
    {
        $converter = new ByteaConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_to_database() : void
    {
        $converter = new ByteaConverter();
        $text = 'simple text';

        $dbValue = $converter->toDatabase($text);
        self::assertNotNull($dbValue);
        self::assertSame($text, $dbValue);
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
}
