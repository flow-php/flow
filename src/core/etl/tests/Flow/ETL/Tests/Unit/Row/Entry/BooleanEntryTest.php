<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\boolean_entry;

final class BooleanEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
    {
        yield 'equal names and values' => [true, boolean_entry('name', true), boolean_entry('name', true)];
        yield 'different names and values' => [
            false,
            boolean_entry('name', true),
            boolean_entry('different_name', true),
        ];
        yield 'equal names and different values' => [false, boolean_entry('name', true), boolean_entry('name', false)];
        yield 'different names characters and equal values' => [
            false,
            boolean_entry('NAME', true),
            boolean_entry('name', true),
        ];
    }

    public function test_duplicating_entry(): void
    {
        $entry = boolean_entry('entry-name', true);
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertEquals($entry, $duplicated);
    }

    public function test_entry_name_can_be_zero(): void
    {
        static::assertSame('0', boolean_entry('0', true)->name());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, BooleanEntry $entry, BooleanEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map(): void
    {
        $entry = boolean_entry('entry-name', true);

        static::assertEquals($entry, $entry->map(static fn(?bool $value): ?bool => $value));
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        boolean_entry('', true);
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = boolean_entry('old_name', true, $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertTrue($renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $entry = boolean_entry('entry-name', true);
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertTrue($newEntry->value());
    }
}
