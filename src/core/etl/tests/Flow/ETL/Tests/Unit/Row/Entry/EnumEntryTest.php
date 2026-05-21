<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\enum_schema;

final class EnumEntryTest extends FlowTestCase
{
    public function test_creating_backed_int_enum_entry(): void
    {
        $value = enum_entry('enum', BackedIntEnum::one)->value();

        static::assertInstanceOf(BackedIntEnum::class, $value);
        static::assertSame(BackedIntEnum::one, $value);
        static::assertSame(1, $value->value);
    }

    public function test_creating_backed_string_enum_entry(): void
    {
        $value = enum_entry('enum', BackedStringEnum::one)->value();

        static::assertInstanceOf(BackedStringEnum::class, $value);
        static::assertSame(BackedStringEnum::one, $value);
        static::assertSame('one', $value->value);
    }

    public function test_creating_basic_enum_entry(): void
    {
        $enum = enum_entry('enum', BasicEnum::one);

        static::assertSame(BasicEnum::one, $enum->value());
        static::assertSame('enum', $enum->name());
    }

    public function test_definition(): void
    {
        static::assertEquals(
            enum_schema('enum', BackedStringEnum::class),
            enum_entry('enum', BackedStringEnum::one)->definition(),
        );
    }

    public function test_is_equal(): void
    {
        static::assertTrue(enum_entry('enum', BasicEnum::one)->isEqual(enum_entry('enum', BasicEnum::one)));
        static::assertFalse(enum_entry('enum', BasicEnum::one)->isEqual(enum_entry('enum', BackedStringEnum::one)));
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = enum_entry('old_name', BasicEnum::one, $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertSame(BasicEnum::one, $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_to_string(): void
    {
        static::assertSame('one', enum_entry('enum', BasicEnum::one)->toString());
        static::assertSame('one', enum_entry('enum', BackedStringEnum::one)->toString());
        static::assertSame('one', enum_entry('enum', BackedIntEnum::one)->toString());
    }
}
