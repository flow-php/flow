<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\Type\Uuid;
use Flow\ETL\Row\Entry\UuidEntry;
use PHPUnit\Framework\TestCase;

final class UuidEntryTest extends TestCase
{
    public static function is_equal_data_provider() : \Generator
    {
        yield 'equal names and values' => [
            true,
            new UuidEntry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            new UuidEntry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
        ];
        yield 'different names and values' => [
            false,
            new UuidEntry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            new UuidEntry('different_name', Uuid::fromString('11111111-1111-1111-1111-111111111111')),
        ];
        yield 'equal names and different values' => [
            false,
            new UuidEntry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            new UuidEntry('name', Uuid::fromString('11111111-1111-1111-1111-111111111111')),
        ];
        yield 'different names characters and equal values' => [
            false,
            new UuidEntry('NAME', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
            new UuidEntry('name', Uuid::fromString('00000000-0000-0000-0000-000000000000')),
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
            $this->markTestSkipped("Package 'ramsey/uuid' or 'symfony/uid' is required for this test.");
        }
    }

    /**
     * @dataProvider valid_string_entries
     */
    public function test_creates_uuid_entry_from_string(string $value) : void
    {
        $entry = UuidEntry::from('entry-name', $value);

        $this->assertEquals($value, $entry->value()->toString());
    }

    /**
     * @dataProvider is_equal_data_provider
     */
    public function test_is_equal(bool $equals, UuidEntry $entry, UuidEntry $nextEntry) : void
    {
        $this->assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map() : void
    {
        $entry = new UuidEntry('entry-name', Uuid::fromString('00000000-0000-0000-0000-000000000000'));

        $this->assertEquals(
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

        new UuidEntry('', Uuid::fromString('00000000-0000-0000-0000-000000000000'));
    }

    public function test_renames_entry() : void
    {
        $entry = new UuidEntry('entry-name', $uuid = Uuid::fromString('00000000-0000-0000-0000-000000000000'));
        /** @var UuidEntry $newEntry */
        $newEntry = $entry->rename('new-entry-name');

        $this->assertEquals('new-entry-name', $newEntry->name());
        $this->assertEquals($uuid->toString(), $newEntry->value()->toString());
    }
}
