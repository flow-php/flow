<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\IntArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class IntArrayConverterTest extends TestCase
{
    public function test_empty_array() : void
    {
        $converter = new IntArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
    }

    public function test_integer_array_to_database() : void
    {
        $converter = new IntArrayConverter();
        $array = [1, 2, 3];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{1,2,3}', $dbValue);
    }

    public function test_invalid_element_type_throws_exception() : void
    {
        $converter = new IntArrayConverter();

        $this->expectException(ValueConversionException::class);
        $converter->toDatabase([1, 'not an integer', 3]);
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new IntArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_element_in_array() : void
    {
        $converter = new IntArrayConverter();
        $array = [1, null, 3];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{1,NULL,3}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new IntArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new IntArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::INT2_ARRAY, $types);
        self::assertContains(PostgreSqlType::INT4_ARRAY, $types);
        self::assertContains(PostgreSqlType::INT8_ARRAY, $types);
        self::assertCount(3, $types);
    }
}
