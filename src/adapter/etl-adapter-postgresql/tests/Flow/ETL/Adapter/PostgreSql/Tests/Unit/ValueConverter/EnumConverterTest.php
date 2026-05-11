<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\PostgreSql\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Adapter\PostgreSql\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Adapter\PostgreSql\Tests\Fixtures\Enum\UnitEnum;
use Flow\ETL\Adapter\PostgreSql\ValueConverter\EnumConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class EnumConverterTest extends TestCase
{
    public function test_backed_int_enum_returns_value_as_string(): void
    {
        $converter = new EnumConverter();

        static::assertSame('1', $converter->toDatabase(BackedIntEnum::one));
        static::assertSame('3', $converter->toDatabase(BackedIntEnum::three));
    }

    public function test_backed_string_enum_returns_value(): void
    {
        $converter = new EnumConverter();

        static::assertSame('one', $converter->toDatabase(BackedStringEnum::one));
        static::assertSame('three', $converter->toDatabase(BackedStringEnum::three));
    }

    public function test_delegates_non_enum_to_next_converter(): void
    {
        $converter = new EnumConverter();

        static::assertSame('plain string', $converter->toDatabase('plain string'));
    }

    public function test_null_returns_null(): void
    {
        $converter = new EnumConverter();

        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_returns_text(): void
    {
        $converter = new EnumConverter();

        static::assertSame([ValueType::TEXT], $converter->supportedTypes());
    }

    public function test_unit_enum_returns_name(): void
    {
        $converter = new EnumConverter();

        static::assertSame('First', $converter->toDatabase(UnitEnum::First));
        static::assertSame('Second', $converter->toDatabase(UnitEnum::Second));
    }
}
