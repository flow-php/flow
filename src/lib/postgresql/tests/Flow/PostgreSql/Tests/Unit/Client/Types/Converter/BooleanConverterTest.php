<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\BooleanConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class BooleanConverterTest extends TestCase
{
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

    public function test_supported_types() : void
    {
        $converter = new BooleanConverter();
        self::assertContains(PostgreSqlType::BOOL, $converter->supportedTypes());
    }

    public function test_true_to_database() : void
    {
        $converter = new BooleanConverter();
        self::assertSame('t', $converter->toDatabase(true));
    }
}
