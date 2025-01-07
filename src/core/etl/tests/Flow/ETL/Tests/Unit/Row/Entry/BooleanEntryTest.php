<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\boolean_entry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class BooleanEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, boolean_entry('name', true), boolean_entry('name', true)];
        yield 'different names and values' => [false, boolean_entry('name', true), boolean_entry('different_name', true)];
        yield 'equal names and different values' => [false, boolean_entry('name', true), boolean_entry('name', false)];
        yield 'different names characters and equal values' => [false, boolean_entry('NAME', true), boolean_entry('name', true)];
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (boolean_entry('0', true))->name());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, BooleanEntry $entry, BooleanEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = boolean_entry('entry-name', true);

        self::assertEquals(
            $entry,
            $entry->map(fn (bool $value) => $value)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        boolean_entry('', true);
    }

    public function test_renames_entry() : void
    {
        $entry = boolean_entry('entry-name', true);
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertTrue($newEntry->value());
    }
}
