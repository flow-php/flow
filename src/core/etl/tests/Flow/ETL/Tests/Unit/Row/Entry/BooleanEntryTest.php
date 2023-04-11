<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\BooleanEntry;
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

    public static function valid_false_entries() : \Generator
    {
        yield [false];
        yield [0];
        yield ['0'];
        yield ['false'];
        yield ['no'];
    }

    public static function valid_true_entries() : \Generator
    {
        yield [true];
        yield [1];
        yield ['1'];
        yield ['true'];
        yield ['yes'];
    }

    /**
     * @dataProvider valid_false_entries
     */
    public function test_creates_false_entry_from_not_boolean_values($value) : void
    {
        $entry = BooleanEntry::from('entry-name', $value);

        $this->assertFalse($entry->value());
    }

    /**
     * @dataProvider valid_true_entries
     */
    public function test_creates_true_entry_from_not_boolean_values($value) : void
    {
        $entry = BooleanEntry::from('entry-name', $value);

        $this->assertTrue($entry->value());
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new BooleanEntry('0', true))->name());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, BooleanEntry $entry, BooleanEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new BooleanEntry('entry-name', true);

        $this->assertEquals(
            $entry,
            $entry->map(fn (bool $value) => $value)
        );
    }

    public function test_prevents_from_creating_entry_from_random_value() : void
    {
        $this->expectExceptionMessage('Value "random-value" can\'t be casted to boolean');

        BooleanEntry::from('entry-name', 'random-value');
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

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertTrue($newEntry->value());
    }
}
