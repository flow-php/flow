<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Types\Converter;

use Flow\PostgreSql\Client\Types\Converter\MultirangeConverter;
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

    public function test_string_to_database() : void
    {
        $converter = new MultirangeConverter();
        $multirange = '{[1,5),[10,20)}';

        $dbValue = $converter->toDatabase($multirange);
        self::assertNotNull($dbValue);
        self::assertSame($multirange, $dbValue);
    }

    public function test_supported_types_empty() : void
    {
        $converter = new MultirangeConverter();
        self::assertSame([], $converter->supportedTypes());
    }
}
