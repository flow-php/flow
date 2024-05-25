<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\ObjectEntry;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ObjectEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new ObjectEntry('name', $object = new \stdClass()), new ObjectEntry('name', $object)];
        yield 'different names and values' => [false, new ObjectEntry('name', $object = new \stdClass()), new ObjectEntry('different_name', $object)];
        yield 'equal names and different values' => [false, new ObjectEntry('name', new \stdClass()), new ObjectEntry('name', new \ArrayObject())];
        yield 'different names characters and equal values' => [false, new ObjectEntry('NAME', $object = new \stdClass()), new ObjectEntry('name', $object)];
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (new ObjectEntry('0', new \stdClass()))->name());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, ObjectEntry $entry, ObjectEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new ObjectEntry('entry-name', new \stdClass());

        self::assertEquals(
            $entry,
            $entry->map(fn (\stdClass $object) => $object)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new ObjectEntry('', new \stdClass());
    }

    public function test_renames_entry() : void
    {
        $entry = new ObjectEntry('entry-name', new \stdClass());
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertIsObject($newEntry->value());
    }

    public function test_serialization() : void
    {
        $string = new ObjectEntry('name', $object = new \stdClass());

        $serialized = \serialize($string);
        /** @var ObjectEntry $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($string->isEqual($unserialized));
    }
}
