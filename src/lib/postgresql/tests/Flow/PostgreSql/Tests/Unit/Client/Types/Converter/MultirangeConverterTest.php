<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\MultirangeConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class MultirangeConverterTest extends TestCase
{
    public function test_non_string_returns_empty() : void
    {
        $converter = new MultirangeConverter();
        self::assertSame('', $converter->toDatabase(12345));
        self::assertSame('', $converter->toDatabase(['array']));
    }

    public function test_null_handling() : void
    {
        $converter = new MultirangeConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_string_passthrough() : void
    {
        $converter = new MultirangeConverter();
        $multirange = '{[1,5),[10,20)}';

        $dbValue = $converter->toDatabase($multirange);
        self::assertNotNull($dbValue);
        self::assertSame($multirange, $dbValue);

        $phpValue = $converter->toPhp($dbValue, PostgreSqlType::INT4);
        self::assertSame($multirange, $phpValue);
    }

    public function test_supported_types_empty() : void
    {
        $converter = new MultirangeConverter();
        self::assertSame([], $converter->supportedTypes());
    }

    public function test_to_php_returns_string() : void
    {
        $converter = new MultirangeConverter();
        $result = $converter->toPhp('{[2024-01-01,2024-12-31)}', PostgreSqlType::DATE);
        self::assertSame('{[2024-01-01,2024-12-31)}', $result);
    }
}
