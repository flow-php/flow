<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use function Flow\ETL\DSL\uuid_entry;
use Flow\ETL\PHP\Value\Uuid;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class UuidEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [
            true,
            uuid_entry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            uuid_entry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
        ];
        yield 'different names and values' => [
            false,
            uuid_entry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            uuid_entry('different_name', Uuid::fromString('11111111-1111-1111-1111-111111111111')),
        ];
        yield 'equal names and different values' => [
            false,
            uuid_entry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            uuid_entry('name', Uuid::fromString('11111111-1111-1111-1111-111111111111')),
        ];
        yield 'different names characters and equal values' => [
            false,
            uuid_entry('NAME', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            uuid_entry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
        ];
    }

    public static function valid_string_entries() : \Generator
    {
        yield ['00000000-0000-0000-0000-000000000000'];
        yield ['11111111-1111-1111-1111-111111111111'];
        yield ['fa2e03e9-707f-4ebc-a40d-4c3c846fef75'];
        yield ['9a419c18-fc21-4481-9dea-5e9cf057d137'];
    }

    protected function setUp() : void
    {
        if (!\class_exists(\Ramsey\Uuid\Uuid::class) && !\class_exists(\Symfony\Component\Uid\Uuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' or 'symfony/uid' is required for this test.");
        }
    }

    #[DataProvider('valid_string_entries')]
    public function test_creates_uuid_entry_from_string(string $value) : void
    {
        $entry = UuidEntry::from('entry-name', $value);

        self::assertEquals($value, $entry->value()->toString());
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, UuidEntry $entry, UuidEntry $nextEntry) : void
    {
        self::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = uuid_entry('entry-name', Uuid::fromString('00000000-0000-0000-0000-000000000000'));

        self::assertEquals(
            $entry,
            $entry->map(fn ($value) => $value)
        );
    }

    public function test_prevents_from_creating_entry_from_random_value() : void
    {
        $this->expectExceptionMessage("Invalid UUID: 'random-value'");

        UuidEntry::from('entry-name', 'random-value');
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name() : void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        uuid_entry('', Uuid::fromString('00000000-0000-0000-0000-000000000000'));
    }

    public function test_renames_entry() : void
    {
        $entry = uuid_entry('entry-name', $uuid = Uuid::fromString('00000000-0000-0000-0000-000000000000'));
        /** @var UuidEntry $newEntry */
        $newEntry = $entry->rename('new-entry-name');

        self::assertEquals('new-entry-name', $newEntry->name());
        self::assertEquals($uuid->toString(), $newEntry->value()->toString());
    }
}
