<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\float_entry;
use function Flow\Types\DSL\type_instance_of;

final class FloatEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): \Generator
    {
        yield 'equal names and values' => [true, float_entry('name', 1.0), float_entry('name', 1.0)];
        yield 'different names and values' => [false, float_entry('name', 1.0), float_entry('different_name', 1.0)];
        yield 'equal names and different values' => [false, float_entry('name', 1.0), float_entry('name', 2)];
        yield 'different names characters and equal values' => [
            false,
            float_entry('NAME', 1.1),
            float_entry('name', 1.1),
        ];
        yield 'different names characters and equal values with high precision' => [
            false,
            float_entry('NAME', 1.00001),
            float_entry('name', 1.00001),
        ];
        yield 'different names characters and different values with high precision' => [
            false,
            float_entry('NAME', 1.205502),
            float_entry('name', 1.205501),
        ];
    }

    public function test_duplicating_entry(): void
    {
        $entry = float_entry('float', 1.0);
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertEquals($entry, $duplicated);
    }

    public function test_entry_name_can_be_zero(): void
    {
        static::assertSame('0', float_entry('0', 0)->name());
        static::assertSame(0.0, float_entry('0', 0)->value());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, FloatEntry $entry, FloatEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map(): void
    {
        $float = float_entry('entry-name', 1);

        static::assertEquals($float, $float->map(static fn(?float $float): ?float => $float));
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        float_entry('', 10.01);
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = float_entry('old_name', 100.5, $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertSame(100.5, $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $float = float_entry('entry-name', 100.00001);
        $newEntry = $float->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals(100.00001, $newEntry->value());
    }

    public function test_serialization(): void
    {
        $float = float_entry('name', 1.0);

        $serialized = \serialize($float);
        /** @var FloatEntry $unserialized */
        $unserialized = type_instance_of(FloatEntry::class)->assert(\unserialize($serialized));

        static::assertTrue($float->isEqual($unserialized));
    }
}
