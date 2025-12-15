<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\BooleanConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class BooleanConverterTest extends TestCase
{
    public function test_0_to_php() : void
    {
        $converter = new BooleanConverter();
        self::assertFalse($converter->toPhp('0', PostgreSqlType::BOOL));
    }

    public function test_1_to_php() : void
    {
        $converter = new BooleanConverter();
        self::assertTrue($converter->toPhp('1', PostgreSqlType::BOOL));
    }

    public function test_f_to_php() : void
    {
        $converter = new BooleanConverter();
        self::assertFalse($converter->toPhp('f', PostgreSqlType::BOOL));
    }

    public function test_false_string_to_php() : void
    {
        $converter = new BooleanConverter();
        self::assertFalse($converter->toPhp('false', PostgreSqlType::BOOL));
    }

    public function test_false_to_database() : void
    {
        $converter = new BooleanConverter();
        self::assertSame('f', $converter->toDatabase(false));
    }

    public function test_null_handling() : void
    {
        $converter = new BooleanConverter();
        self::assertNull($converter->toDatabase(null));
    }

    public function test_round_trip_conversion() : void
    {
        $converter = new BooleanConverter();

        $dbValue = $converter->toDatabase(true);
        self::assertNotNull($dbValue);
        self::assertTrue($converter->toPhp($dbValue, PostgreSqlType::BOOL));

        $dbValue = $converter->toDatabase(false);
        self::assertNotNull($dbValue);
        self::assertFalse($converter->toPhp($dbValue, PostgreSqlType::BOOL));
    }

    public function test_supported_types() : void
    {
        $converter = new BooleanConverter();
        self::assertContains(PostgreSqlType::BOOL, $converter->supportedTypes());
    }

    public function test_t_to_php() : void
    {
        $converter = new BooleanConverter();
        self::assertTrue($converter->toPhp('t', PostgreSqlType::BOOL));
    }

    public function test_true_string_to_php() : void
    {
        $converter = new BooleanConverter();
        self::assertTrue($converter->toPhp('true', PostgreSqlType::BOOL));
    }

    public function test_true_to_database() : void
    {
        $converter = new BooleanConverter();
        self::assertSame('t', $converter->toDatabase(true));
    }
}
