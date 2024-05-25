<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\DateTimeEntry;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DateTimeEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00'))];
        yield 'different names and values' => [false, new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00')), new DateTimeEntry('different_name', new \DateTimeImmutable('2020-01-01 00:00:00+00'))];
        yield 'equal names and different values day' => [false, new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-01-02 00:00:00+00'))];
        yield 'equal names and different values hour' => [false, new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 02:00:00+00'))];
        yield 'equal names and different values tz' => [false, new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+10'))];
        yield 'different names characters and equal values' => [false, new DateTimeEntry('NAME', new \DateTimeImmutable('2020-01-01 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00'))];
        yield 'equal names and equal values and different format' => [false, new DateTimeEntry('name', new \DateTimeImmutable('2020-02-19 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-01-02 00:00:00+00'))];
        yield 'equal names and equal values for given format' => [true, new DateTimeEntry('name', new \DateTimeImmutable('2020-02-19 00:00:00+00')), new DateTimeEntry('name', new \DateTimeImmutable('2020-02-19 00:00:00+00'))];
    }

    public function test_entry_name_can_be_zero() : void
    {
        self::assertSame('0', (new DateTimeEntry('0', new \DateTimeImmutable('2020-07-13 12:00')))->name());
    }

    public function test_invalid_date() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Invalid value given: 'random string', reason: Failed to parse time string (random string) at position 0 (r): The timezone could not be found in the database");

        new DateTimeEntry('a', 'random string');
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, DateTimeEntry $entry, DateTimeEntry $nextEntry) : void
    {
        self::assertEquals($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new DateTimeEntry('entry-name', new \DateTimeImmutable());

        self::assertEquals(
            $entry,
            $entry->map(fn (\DateTimeImmutable $dateTimeImmutable) => $dateTimeImmutable)
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        new DateTimeEntry('', new \DateTimeImmutable('2020-07-13 12:00'));
    }

    public function test_renames_entry() : void
    {
        $entry = new DateTimeEntry('entry-name', new \DateTimeImmutable());
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_serialization() : void
    {
        $string = new DateTimeEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00+00'));

        $serialized = \serialize($string);
        /** @var DateTimeEntry $unserialized */
        $unserialized = \unserialize($serialized);

        self::assertTrue($string->isEqual($unserialized));
    }

    public function test_uses_full_date_time() : void
    {
        $entry = new DateTimeEntry('entry-name', new \DateTimeImmutable('2020-07-13 12:00'));

        self::assertEquals($entry->value(), new \DateTimeImmutable('2020-07-13 12:00'));
    }
}
