<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\integer_entry;
use function serialize;
use function unserialize;

final class IntegerEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
    {
        yield 'equal names and values' => [true, integer_entry('name', 1), integer_entry('name', 1)];
        yield 'different names and values' => [false, integer_entry('name', 1), integer_entry('different_name', 1)];
        yield 'equal names and different values' => [false, integer_entry('name', 1), integer_entry('name', 2)];
        yield 'different names characters and equal values' => [
            false,
            integer_entry('NAME', 1),
            integer_entry('name', 1),
        ];
    }

    public function test_entry_name_can_be_zero(): void
    {
        static::assertSame('0', integer_entry('0', 0)->name());
    }

    /**
     * @param IntegerEntry<int|null> $entry
     * @param IntegerEntry<int|null> $nextEntry
     */
    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, IntegerEntry $entry, IntegerEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        integer_entry('', 100);
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = integer_entry('old_name', 100, $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertSame(100, $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $entry = integer_entry('entry-name', 100);
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals(100, $newEntry->value());
    }

    public function test_serialization(): void
    {
        $string = integer_entry('name', 1);

        $serialized = serialize($string);
        /** @var IntegerEntry<int|null> $unserialized */
        $unserialized = unserialize($serialized);

        static::assertTrue($string->isEqual($unserialized));
    }
}
