<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\Converter\TextArrayConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class TextArrayConverterTest extends TestCase
{
    public function test_empty_array() : void
    {
        $converter = new TextArrayConverter();
        self::assertSame('{}', $converter->toDatabase([]));
    }

    public function test_invalid_element_type_throws_exception() : void
    {
        $converter = new TextArrayConverter();

        $this->expectException(ValueConversionException::class);
        $converter->toDatabase(['a', ['nested' => 'array'], 'c']);
    }

    public function test_non_array_returns_empty_braces() : void
    {
        $converter = new TextArrayConverter();
        self::assertSame('{}', $converter->toDatabase('not an array'));
        self::assertSame('{}', $converter->toDatabase(12345));
    }

    public function test_null_element_in_array() : void
    {
        $converter = new TextArrayConverter();
        $array = ['a', null, 'c'];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{a,NULL,c}', $dbValue);
    }

    public function test_null_handling() : void
    {
        $converter = new TextArrayConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_simple_text_array_to_database() : void
    {
        $converter = new TextArrayConverter();
        $array = ['a', 'b', 'c'];

        $dbValue = $converter->toDatabase($array);
        self::assertSame('{a,b,c}', $dbValue);
    }

    public function test_supported_types() : void
    {
        $converter = new TextArrayConverter();
        $types = $converter->supportedTypes();

        self::assertContains(PostgreSqlType::TEXT_ARRAY, $types);
        self::assertContains(PostgreSqlType::VARCHAR_ARRAY, $types);
        self::assertCount(2, $types);
    }
}
