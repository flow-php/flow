<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Tests\Fixtures\Enum\{BackedIntEnum, BackedStringEnum, BasicEnum};
use PHPUnit\Framework\TestCase;

final class EnumEntryTest extends TestCase
{
    public function test_creating_backed_int_enum_entry() : void
    {
        $enum = new EnumEntry('enum', BackedIntEnum::one);

        self::assertSame(
            BackedIntEnum::one,
            $enum->value(),
        );
        self::assertSame(
            1,
            $enum->value()->value,
        );
    }

    public function test_creating_backed_string_enum_entry() : void
    {
        $enum = new EnumEntry('enum', BackedStringEnum::one);

        self::assertSame(
            BackedStringEnum::one,
            $enum->value(),
        );
        self::assertSame(
            'one',
            $enum->value()->value,
        );
    }

    public function test_creating_basic_enum_entry() : void
    {
        $enum = new EnumEntry('enum', BasicEnum::one);

        self::assertSame(
            BasicEnum::one,
            $enum->value(),
        );
        self::assertSame('enum', $enum->name());
    }

    public function test_definition() : void
    {
        self::assertEquals(
            Definition::enum(
                'enum',
                BackedStringEnum::class
            ),
            (new EnumEntry('enum', BackedStringEnum::one))->definition()
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            (new EnumEntry('enum', BasicEnum::one))->isEqual(new EnumEntry('enum', BasicEnum::one)),
        );
        self::assertFalse(
            (new EnumEntry('enum', BasicEnum::one))->isEqual(new EnumEntry('enum', BackedStringEnum::one)),
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'one',
            (new EnumEntry('enum', BasicEnum::one))->toString()
        );
        self::assertSame(
            'one',
            (new EnumEntry('enum', BackedStringEnum::one))->toString()
        );
        self::assertSame(
            'one',
            (new EnumEntry('enum', BackedIntEnum::one))->toString()
        );
    }
}
