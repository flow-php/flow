<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\date_entry;
use function serialize;
use function unserialize;

final class DateEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
    {
        yield 'equal names and values' => [
            true,
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
        ];
        yield 'different names and values' => [
            false,
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
            date_entry('different_name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
        ];
        yield 'equal names and different values day' => [
            false,
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
            date_entry('name', new DateTimeImmutable('2020-01-02 00:00:00+00')),
        ];
        yield 'equal names and different values tz' => [
            false,
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+10')),
        ];
        yield 'different names characters and equal values' => [
            false,
            date_entry('NAME', new DateTimeImmutable('2020-01-01 00:00:00+00')),
            date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00')),
        ];
        yield 'equal names and equal values and different format' => [
            false,
            date_entry('name', new DateTimeImmutable('2020-02-19 00:00:00+00')),
            date_entry('name', new DateTimeImmutable('2020-01-02 00:00:00+00')),
        ];
        yield 'equal names and equal values for given format' => [
            true,
            date_entry('name', new DateTimeImmutable('2020-02-19 00:00:00+00')),
            date_entry('name', new DateTimeImmutable('2020-02-19 00:00:00+00')),
        ];
    }

    public function test_duplicating_entry(): void
    {
        $entry = date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00'));
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertEquals($entry, $duplicated);
    }

    public function test_entry_name_can_be_zero(): void
    {
        static::assertSame('0', date_entry('0', new DateTimeImmutable('2020-07-13 12:00'))->name());
    }

    public function test_invalid_date(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "Invalid value given: 'random string', reason: Failed to parse time string (random string) at position 0 (r): The timezone could not be found in the database",
        );

        date_entry('a', 'random string');
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, DateEntry $entry, DateEntry $nextEntry): void
    {
        static::assertEquals($equals, $entry->isEqual($nextEntry));
    }

    public function test_map(): void
    {
        $entry = date_entry('entry-name', new DateTimeImmutable());

        static::assertEquals(
            $entry,
            $entry->map(static fn(?DateTimeInterface $dateTimeImmutable): ?DateTimeInterface => $dateTimeImmutable),
        );
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry name cannot be empty');

        date_entry('', new DateTimeImmutable('2020-07-13 12:00'));
    }

    public function test_removes_time(): void
    {
        static::assertEquals(
            date_entry('entry-name', new DateTimeImmutable('2020-07-13 12:00'))->value(),
            new DateTimeImmutable('2020-07-13 00:00'),
        );
        static::assertEquals(
            date_entry('entry-name', '2020-07-13 12:00')->value(),
            new DateTimeImmutable('2020-07-13 00:00'),
        );
        static::assertEquals(
            date_entry('entry-name', new DateTime('2020-07-13 12:00'))->value(),
            new DateTimeImmutable('2020-07-13 00:00'),
        );
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = date_entry('old_name', new DateTimeImmutable('2020-01-01'), $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertEquals($entry->value(), $renamedEntry->value());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $entry = date_entry('entry-name', new DateTimeImmutable());
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals($entry->value(), $newEntry->value());
    }

    public function test_serialization(): void
    {
        $string = date_entry('name', new DateTimeImmutable('2020-01-01 00:00:00+00'));

        $serialized = serialize($string);
        /** @var DateEntry $unserialized */
        $unserialized = unserialize($serialized);

        static::assertTrue($string->isEqual($unserialized));
    }
}
