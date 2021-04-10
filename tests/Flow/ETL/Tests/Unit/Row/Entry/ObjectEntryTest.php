<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\ObjectEntry;
use PHPUnit\Framework\TestCase;

final class ObjectEntryTest extends TestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new ObjectEntry('', new \stdClass());
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new ObjectEntry('0', new \stdClass()))->name());
    }

    public function test_renames_entry() : void
    {
        $entry = new ObjectEntry('entry-name', new \stdClass());
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertIsObject($newEntry->value());
    }

    public function test_map() : void
    {
        $entry = new ObjectEntry('entry-name', new \stdClass());

        $this->assertEquals(
            $entry,
            $entry->map(function (\stdClass $object) {
                return $object;
            })
        );
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, ObjectEntry $entry, ObjectEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new ObjectEntry('name', $object = new \stdClass()), new ObjectEntry('name', $object)];
        yield 'different names and values' => [false, new ObjectEntry('name', $object = new \stdClass()), new ObjectEntry('different_name', $object)];
        yield 'equal names and different values' => [false, new ObjectEntry('name', new \stdClass()), new ObjectEntry('name', new \ArrayObject())];
        yield 'equal names and different value characters' => [false, new ObjectEntry('name', new \stdClass()), new ObjectEntry('name', new \stdClass())];
        yield 'different names characters and equal values' => [true, new ObjectEntry('NAME', $object = new \stdClass()), new ObjectEntry('name', $object)];
    }
}
