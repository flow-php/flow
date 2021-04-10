<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\DateEntry;
use PHPUnit\Framework\TestCase;

final class DateEntryTest extends TestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        new DateEntry('', new \DateTimeImmutable('2020-07-13 12:00'));
    }

    public function test_entry_name_can_be_zero() : void
    {
        $this->assertSame('0', (new DateEntry('0', new \DateTimeImmutable('2020-07-13 12:00')))->name());
    }

    public function test_trims_time_to_begin_of_a_day() : void
    {
        $entry = new DateEntry('entry-name', new \DateTimeImmutable('2020-07-13 12:00'));

        $this->assertEquals(new \DateTimeImmutable('2020-07-13 00:00'), new \DateTimeImmutable($entry->value()));
    }

    public function test_renames_entry() : void
    {
        $entry = new DateEntry('entry-name', new \DateTimeImmutable());
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($entry->value(), $newEntry->value());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, DateEntry $entry, DateEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new DateEntry('entry-name', new \DateTimeImmutable());

        $this->assertEquals(
            $entry,
            $entry->map(function (\DateTimeImmutable $dateTimeImmutable) {
                return $dateTimeImmutable;
            })
        );
    }

    public function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [true, new DateEntry('name', new \DateTimeImmutable('2020-01-01')), new DateEntry('name', new \DateTimeImmutable('2020-01-01'))];
        yield 'different names and values' => [false, new DateEntry('name', new \DateTimeImmutable('2020-01-01')), new DateEntry('different_name', new \DateTimeImmutable('2020-01-01'))];
        yield 'equal names and different values' => [false, new DateEntry('name', new \DateTimeImmutable('2020-01-01')), new DateEntry('name', new \DateTimeImmutable('2020-01-11'))];
        yield 'equal names and different hours' => [true, new DateEntry('name', new \DateTimeImmutable('2020-01-01 00:00:00')), new DateEntry('name', new \DateTimeImmutable('2020-01-01 01:00:01'))];
        yield 'different names characters and equal values' => [true, new DateEntry('NAME', new \DateTimeImmutable('2020-01-01')), new DateEntry('name', new \DateTimeImmutable('2020-01-01'))];
    }
}
