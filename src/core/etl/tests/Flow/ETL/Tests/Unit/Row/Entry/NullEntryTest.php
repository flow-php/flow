<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\NullType;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;

final class NullEntryTest extends FlowTestCase
{
    public function test_null_entry_dsl_returns_a_null_entry(): void
    {
        static::assertInstanceOf(NullEntry::class, null_entry('e'));
    }

    public function test_definition_is_a_null_definition(): void
    {
        static::assertInstanceOf(NullDefinition::class, null_entry('e')->definition());
    }

    public function test_empty_name_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        null_entry('');
    }

    public function test_is_equal_to_another_null_entry_with_the_same_name(): void
    {
        static::assertTrue(null_entry('e')->isEqual(null_entry('e')));
    }

    public function test_is_not_equal_to_a_non_null_entry(): void
    {
        static::assertFalse(null_entry('e')->isEqual(int_entry('e', 1)));
    }

    public function test_is_not_equal_to_a_null_entry_with_a_different_name(): void
    {
        static::assertFalse(null_entry('e')->isEqual(null_entry('other')));
    }

    public function test_metadata_is_carried_into_the_definition(): void
    {
        static::assertTrue(null_entry('e', Metadata::with('k', 'v'))->definition()->metadata()->has('k'));
        static::assertSame('v', null_entry('e', Metadata::with('k', 'v'))->definition()->metadata()->get('k'));
    }

    public function test_name(): void
    {
        static::assertSame('e', null_entry('e')->name());
    }

    public function test_rename_keeps_metadata(): void
    {
        $renamed = null_entry('e', Metadata::with('k', 'v'))->rename('renamed');

        static::assertSame('renamed', $renamed->name());
        static::assertTrue($renamed->definition()->metadata()->has('k'));
    }

    public function test_to_string_is_empty(): void
    {
        static::assertSame('', null_entry('e')->toString());
        static::assertSame('', (string) null_entry('e'));
    }

    public function test_type_is_null_type(): void
    {
        static::assertInstanceOf(NullType::class, null_entry('e')->type());
    }

    public function test_value_is_null(): void
    {
        static::assertNull(null_entry('e')->value());
    }
}
