<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\float_entry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class FloatEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, float_entry('name', 1.0), float_entry('name', 1.0)];
        yield 'different names and values' => [false, float_entry('name', 1.0), float_entry('different_name', 1.0)];
        yield 'equal names and different values' => [false, float_entry('name', 1.0), float_entry('name', 2)];
        yield 'different names characters and equal values' => [false, float_entry('NAME', 1.1), float_entry('name', 1.1)];
        yield 'different names characters and equal values with high precision' => [false, float_entry('NAME', 1.00001), float_entry('name', 1.00001)];
        yield 'different names characters and different values with high precision' => [false, float_entry('NAME', 1.205502), float_entry('name', 1.205501)];
    }

    public function test_accessing_precission() : void
    {
        self::assertSame(6, float_entry('name', 1.0)->precision);
        self::assertSame(3, float_entry('name', 1.0, 3)->precision);
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (float_entry('0', 0))->name());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, FloatEntry $entry, FloatEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $float = float_entry('entry-name', 1);

        self::assertEquals(
            $float,
            $float->map(fn (float $float) => $float)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        float_entry('', 10.01);
    }

    public function test_renames_entry() : void
    {
        $float = float_entry('entry-name', 100.00001);
        $newEntry = $float->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals(100.00001, $newEntry->value());
    }

    public function test_serialization() : void
    {
        $float = float_entry('name', 1.0);

        $serialized = \serialize($float);
        /** @var FloatEntry $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($float->isEqual($unserialized));
    }
}
