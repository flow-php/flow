<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\BooleanEntry;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BooleanEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new BooleanEntry('name', true), new BooleanEntry('name', true)];
        yield 'different names and values' => [false, new BooleanEntry('name', true), new BooleanEntry('different_name', true)];
        yield 'equal names and different values' => [false, new BooleanEntry('name', true), new BooleanEntry('name', false)];
        yield 'different names characters and equal values' => [false, new BooleanEntry('NAME', true), new BooleanEntry('name', true)];
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (new BooleanEntry('0', true))->name());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, BooleanEntry $entry, BooleanEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new BooleanEntry('entry-name', true);

        self::assertEquals(
            $entry,
            $entry->map(fn (bool $value) => $value)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new BooleanEntry('', true);
    }

    public function test_renames_entry() : void
    {
        $entry = new BooleanEntry('entry-name', true);
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertTrue($newEntry->value());
    }
}
