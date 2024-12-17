<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class StringEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, string_entry('name', 'value'), string_entry('name', 'value')];
        yield 'different names and values' => [false, string_entry('name', 'value'), string_entry('different_name', 'value')];
        yield 'equal names and different values' => [false, string_entry('name', 'value'), string_entry('name', 'different_value')];
        yield 'equal names and different value characters' => [false, string_entry('name', 'value'), string_entry('name', 'VALUE')];
        yield 'different names characters and equal values' => [false, string_entry('NAME', 'value'), string_entry('name', 'value')];
    }

    public function test_creates_lowercase_value() : void
    {
        $entry = StringEntry::lowercase('lowercase', 'It Should Be Lowercase');

        self::assertEquals('it should be lowercase', $entry->value());
    }

    public function test_creates_uppercase_value() : void
    {
        $entry = StringEntry::uppercase('uppercase', 'It Should Be Uppercase');

        self::assertEquals('IT SHOULD BE UPPERCASE', $entry->value());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, StringEntry $entry, StringEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = string_entry('entry-name', 'any string value');

        self::assertEquals(
            $entry,
            $entry->map(fn (string $value) => $value)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        string_entry('', 'any string value');
    }

    public function test_renames_entry() : void
    {
        $entry = string_entry('entry-name', 'any string value');
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_serialization() : void
    {
        $string = string_entry('name', <<<'TXT'
This is some very long
multi-line string, including different values like: ąćżźą

TXT);

        $serialized = \serialize($string);
        $unserialized = \unserialize($serialized);

        self::assertTrue($string->isEqual($unserialized));
    }
}
