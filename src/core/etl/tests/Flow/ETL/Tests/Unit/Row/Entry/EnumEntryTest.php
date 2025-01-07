<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\{enum_entry, enum_schema};
use Flow\ETL\Tests\Fixtures\Enum\{BackedIntEnum, BackedStringEnum, BasicEnum};
use Flow\ETL\Tests\FlowTestCase;

final class EnumEntryTest extends FlowTestCase
{
    public function test_creating_backed_int_enum_entry() : void
    {
        $enum = enum_entry('enum', BackedIntEnum::one);

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
        $enum = enum_entry('enum', BackedStringEnum::one);

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
        $enum = enum_entry('enum', BasicEnum::one);

        self::assertSame(
            BasicEnum::one,
            $enum->value(),
        );
        self::assertSame('enum', $enum->name());
    }

    public function test_definition() : void
    {
        self::assertEquals(
            enum_schema('enum', BackedStringEnum::class),
            (enum_entry('enum', BackedStringEnum::one))->definition()
        );
    }

    public function test_is_equal() : void
    {
        self::assertTrue(
            (enum_entry('enum', BasicEnum::one))->isEqual(enum_entry('enum', BasicEnum::one)),
        );
        self::assertFalse(
            (enum_entry('enum', BasicEnum::one))->isEqual(enum_entry('enum', BackedStringEnum::one)),
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'one',
            (enum_entry('enum', BasicEnum::one))->toString()
        );
        self::assertSame(
            'one',
            (enum_entry('enum', BackedStringEnum::one))->toString()
        );
        self::assertSame(
            'one',
            (enum_entry('enum', BackedIntEnum::one))->toString()
        );
    }
}
