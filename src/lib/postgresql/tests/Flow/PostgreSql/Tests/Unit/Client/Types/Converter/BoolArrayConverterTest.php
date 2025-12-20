<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\BoolArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class BoolArrayConverterTest extends TestCase
{
    public function test_boolean_array_to_database() : void
    {
        $converter = new BoolArrayConverter();
        $array = [true, false, true];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{t,f,t}', $dbValue);
    }

    public function test_empty_array() : void
    {
        $converter = new BoolArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
    }

    public function test_invalid_element_type_throws_exception() : void
    {
        $converter = new BoolArrayConverter();

        $this->expectException(ValueConversionException::class);
        $converter->toDatabase([true, 'not a boolean', false]);
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new BoolArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_element_in_array() : void
    {
        $converter = new BoolArrayConverter();
        $array = [true, null, false];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{t,NULL,f}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new BoolArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types() : void
    {
        $converter = new BoolArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::BOOL_ARRAY, $types);
        self::assertCount(1, $types);
    }
}
