<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\IntegerEntry;
use PHPUnit\Framework\TestCase;

final class IntegerEntryTest extends TestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new IntegerEntry('', 100);
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new IntegerEntry('0', 0))->name());
    }

    public function test_renames_entry() : void
    {
        $entry = new IntegerEntry('entry-name', 100);
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals(100, $newEntry->value());
    }

    /**
     * @dataProvider invalid_entries
     */
    public function test_prevents_from_creating_entry_from_invalid_entry_values($value) : void
    {
        $this->expectExceptionMessage(\sprintf('Value "%s" can\'t be casted to integer', $value));

        IntegerEntry::from('entry-name', $value);
    }

    /**
     * @return \Generator
     */
    public function invalid_entries() : \Generator
    {
        yield ['random_value'];
        yield [100.50];
        yield ['100.5'];
    }

    /**
     * @dataProvider valid_integer_entries
     */
    public function test_creates_true_entry_from_not_boolean_values($value) : void
    {
        $entry = IntegerEntry::from('entry-name', $value);

        $this->assertEquals((int) $value, $entry->value());
    }

    public function test_map() : void
    {
        $entry = new IntegerEntry('entry-name', 1);

        $this->assertEquals(
            $entry,
            $entry->map(function (int $int) {
                return $int;
            })
        );
    }

    /**
     * @return \Generator
     */
    public function valid_integer_entries() : \Generator
    {
        yield [100];
        yield [100.00];
        yield ['100'];
        yield ['100.00'];
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, IntegerEntry $entry, IntegerEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new IntegerEntry('name', 1), new IntegerEntry('name', 1)];
        yield 'different names and values' => [false, new IntegerEntry('name', 1), new IntegerEntry('different_name', 1)];
        yield 'equal names and different values' => [false, new IntegerEntry('name', 1), new IntegerEntry('name', 2)];
        yield 'different names characters and equal values' => [true, new IntegerEntry('NAME', 1), new IntegerEntry('name', 1)];
    }

    public function test_serialization() : void
    {
        $string = new IntegerEntry('name', 1);

        $serialized = \serialize($string);
        /** @var IntegerEntry $unserialized */
        $unserialized = \unserialize($serialized);

        $this->assertTrue($string->isEqual($unserialized));
    }
}
