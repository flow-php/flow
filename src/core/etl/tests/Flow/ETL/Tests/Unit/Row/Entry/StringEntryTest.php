<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_instance_of;
use function serialize;
use function unserialize;

final class StringEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
    {
        yield 'equal names and values' => [true, string_entry('name', 'value'), string_entry('name', 'value')];
        yield 'different names and values' => [
            false,
            string_entry('name', 'value'),
            string_entry('different_name', 'value'),
        ];
        yield 'equal names and different values' => [
            false,
            string_entry('name', 'value'),
            string_entry('name', 'different_value'),
        ];
        yield 'equal names and different value characters' => [
            false,
            string_entry('name', 'value'),
            string_entry('name', 'VALUE'),
        ];
        yield 'different names characters and equal values' => [
            false,
            string_entry('NAME', 'value'),
            string_entry('name', 'value'),
        ];
    }

    public function test_creates_lowercase_value(): void
    {
        $entry = StringEntry::lowercase('lowercase', 'It Should Be Lowercase');

        static::assertEquals('it should be lowercase', $entry->value());
    }

    public function test_creates_uppercase_value(): void
    {
        $entry = StringEntry::uppercase('uppercase', 'It Should Be Uppercase');

        static::assertEquals('IT SHOULD BE UPPERCASE', $entry->value());
    }

    /**
     * @param StringEntry<string|null> $entry
     * @param StringEntry<string|null> $nextEntry
     */
    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, StringEntry $entry, StringEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        string_entry('', 'any string value');
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = string_entry('old_name', 'test value', $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertSame('test value', $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $entry = string_entry('entry-name', 'any string value');
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_serialization(): void
    {
        $string = string_entry('name', <<<'TXT'
            This is some very long
            multi-line string, including different values like: ąćżźą

            TXT);

        $serialized = serialize($string);
        $unserialized = type_instance_of(StringEntry::class)->assert(unserialize($serialized));

        static::assertTrue($string->isEqual($unserialized));
    }
}
