<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class StringEntryTest extends TestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new StringEntry('', 'any string value');
    }

    public function test_renames_entry() : void
    {
        $entry = new StringEntry('entry-name', 'any string value');
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($entry->value(), $newEntry->value());
    }

    public function test_creates_lowercase_value() : void
    {
        $entry = StringEntry::lowercase('lowercase', 'It Should Be Lowercase');

        $this->assertEquals('it should be lowercase', $entry->value());
    }

    public function test_creates_uppercase_value() : void
    {
        $entry = StringEntry::uppercase('uppercase', 'It Should Be Uppercase');

        $this->assertEquals('IT SHOULD BE UPPERCASE', $entry->value());
    }

    public function test_creates_datetime_value() : void
    {
        $entry = StringEntry::fromDateTime('datetime', new \DateTimeImmutable('2021-06-01 00:00:00 UTC'));

        $this->assertEquals('2021-06-01T00:00:00+00:00', $entry->value());
    }

    public function test_map() : void
    {
        $entry = new StringEntry('entry-name', 'any string value');

        $this->assertEquals(
            $entry,
            $entry->map(function (string $value) {
                return $value;
            })
        );
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, StringEntry $entry, StringEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new StringEntry('name', 'value'), new StringEntry('name', 'value')];
        yield 'different names and values' => [false, new StringEntry('name', 'value'), new StringEntry('different_name', 'value')];
        yield 'equal names and different values' => [false, new StringEntry('name', 'value'), new StringEntry('name', 'different_value')];
        yield 'equal names and different value characters' => [false, new StringEntry('name', 'value'), new StringEntry('name', 'VALUE')];
        yield 'different names characters and equal values' => [true, new StringEntry('NAME', 'value'), new StringEntry('name', 'value')];
    }
}
