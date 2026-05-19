<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use Ramsey\Uuid\Uuid as RamseyUuid;
use Symfony\Component\Uid\Uuid as SymfonyUuid;

use function class_exists;
use function Flow\ETL\DSL\uuid_entry;

final class UuidEntryTest extends FlowTestCase
{
    public static function is_equal_data_provider(): Generator
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

    public static function valid_string_entries(): Generator
    {
        yield ['00000000-0000-0000-0000-000000000000'];
        yield ['11111111-1111-1111-1111-111111111111'];
        yield ['fa2e03e9-707f-4ebc-a40d-4c3c846fef75'];
        yield ['9a419c18-fc21-4481-9dea-5e9cf057d137'];
    }

    protected function setUp(): void
    {
        if (!class_exists(RamseyUuid::class) && !class_exists(SymfonyUuid::class)) {
            self::markTestSkipped("Package 'ramsey/uuid' or 'symfony/uid' is required for this test.");
        }
    }

    #[DataProvider('valid_string_entries')]
    public function test_creates_uuid_entry_from_string(string $value): void
    {
        $entry = UuidEntry::from('entry-name', $value);

        static::assertEquals($value, $entry->value()?->toString());
    }

    public function test_duplicating_entry(): void
    {
        $entry = uuid_entry('entry-name', Uuid::fromString('00000000-0000-0000-0000-000000000000'));
        $duplicated = $entry->duplicate();

        static::assertNotSame($entry, $duplicated);
        static::assertEquals($entry, $duplicated);
    }

    #[DataProvider('is_equal_data_provider')]
    public function test_is_equal(bool $equals, UuidEntry $entry, UuidEntry $nextEntry): void
    {
        static::assertSame($equals, $entry->isEqual($nextEntry));
    }

    public function test_map(): void
    {
        $entry = uuid_entry('entry-name', Uuid::fromString('00000000-0000-0000-0000-000000000000'));

        static::assertEquals($entry, $entry->map(static fn($value) => $value));
    }

    public function test_prevents_from_creating_entry_from_random_value(): void
    {
        $this->expectExceptionMessage("Invalid UUID: 'random-value'");

        UuidEntry::from('entry-name', 'random-value');
    }

    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        uuid_entry('', Uuid::fromString('00000000-0000-0000-0000-000000000000'));
    }

    public function test_rename_preserves_metadata(): void
    {
        $metadata = Metadata::fromArray(['description' => 'test metadata', 'priority' => 1]);
        $entry = uuid_entry('old_name', Uuid::fromString('00000000-0000-0000-0000-000000000000'), $metadata);

        $renamedEntry = $entry->rename('new_name');

        static::assertSame('new_name', $renamedEntry->name());
        static::assertEquals($entry->value()?->toString(), $renamedEntry->value()?->toString());
        static::assertTrue($renamedEntry->definition()->metadata()->isEqual($metadata));
    }

    public function test_renames_entry(): void
    {
        $entry = uuid_entry('entry-name', $uuid = Uuid::fromString('00000000-0000-0000-0000-000000000000'));
        /** @var UuidEntry $newEntry */
        $newEntry = $entry->rename('new-entry-name');

        static::assertEquals('new-entry-name', $newEntry->name());
        static::assertEquals($uuid->toString(), $newEntry->value()?->toString());
    }
}
