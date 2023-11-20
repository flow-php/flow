<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\FloatEntry;
use PHPUnit\Framework\TestCase;

final class FloatEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new FloatEntry('name', 1.0), new FloatEntry('name', 1.0)];
        yield 'different names and values' => [false, new FloatEntry('name', 1.0), new FloatEntry('different_name', 1.0)];
        yield 'equal names and different values' => [false, new FloatEntry('name', 1.0), new FloatEntry('name', 2)];
        yield 'different names characters and equal values' => [false, new FloatEntry('NAME', 1.1), new FloatEntry('name', 1.1)];
        yield 'different names characters and equal values with high precision' => [false, new FloatEntry('NAME', 1.00001), new FloatEntry('name', 1.00001)];
        yield 'different names characters and different values with high precision' => [false, new FloatEntry('NAME', 1.205502), new FloatEntry('name', 1.205501)];
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new FloatEntry('0', 0))->name());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, FloatEntry $entry, FloatEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new FloatEntry('entry-name', 1);

        $this->assertEquals(
            $entry,
            $entry->map(fn (float $float) => $float)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new FloatEntry('', 10.01);
    }

    public function test_renames_entry() : void
    {
        $entry = new FloatEntry('entry-name', 100.00001);
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals(100.00001, $newEntry->value());
    }

    public function test_serialization() : void
    {
        $string = new FloatEntry('name', 1.0);

        $serialized = \serialize($string);
        /** @var FloatEntry $unserialized */
        $unserialized = \unserialize($serialized);

        $this->assertTrue($string->isEqual($unserialized));
    }
}
