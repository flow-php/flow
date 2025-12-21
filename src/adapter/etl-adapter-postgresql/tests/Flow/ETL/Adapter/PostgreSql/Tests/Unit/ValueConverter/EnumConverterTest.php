<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\PostgreSql\Tests\Fixtures\Enum\{BackedIntEnum, BackedStringEnum, UnitEnum};
use Flow\ETL\Adapter\PostgreSql\ValueConverter\EnumConverter;
use Flow\PostgreSql\Client\Types\PostgreSqlType;
use PHPUnit\Framework\TestCase;

final class EnumConverterTest extends TestCase
{
    public function test_backed_int_enum_returns_value_as_string() : void
    {
        $converter = new EnumConverter();

        self::assertSame('1', $converter->toDatabase(BackedIntEnum::one));
        self::assertSame('3', $converter->toDatabase(BackedIntEnum::three));
    }

    public function test_backed_string_enum_returns_value() : void
    {
        $converter = new EnumConverter();

        self::assertSame('one', $converter->toDatabase(BackedStringEnum::one));
        self::assertSame('three', $converter->toDatabase(BackedStringEnum::three));
    }

    public function test_delegates_non_enum_to_next_converter() : void
    {
        $converter = new EnumConverter();

        self::assertSame('plain string', $converter->toDatabase('plain string'));
    }

    public function test_null_returns_null() : void
    {
        $converter = new EnumConverter();

        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_returns_text() : void
    {
        $converter = new EnumConverter();

        self::assertSame([PostgreSqlType::TEXT], $converter->supportedTypes());
    }

    public function test_unit_enum_returns_name() : void
    {
        $converter = new EnumConverter();

        self::assertSame('First', $converter->toDatabase(UnitEnum::First));
        self::assertSame('Second', $converter->toDatabase(UnitEnum::Second));
    }
}
