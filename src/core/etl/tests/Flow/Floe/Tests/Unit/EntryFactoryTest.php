<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\Floe\EntryFactory;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function serialize;

final class EntryFactoryTest extends TestCase
{
    public function test_created_entry_equals_constructed_entry(): void
    {
        $entry = EntryFactory::forEntryClass(IntegerEntry::class)->create('id', 42, new IntegerDefinition('id'));

        static::assertEquals(int_entry('id', 42), $entry);
        static::assertSame(serialize(int_entry('id', 42)), serialize($entry));
    }

    public function test_creating_entry_with_null_value(): void
    {
        $entry = EntryFactory::forEntryClass(StringEntry::class)->create(
            'name',
            null,
            new StringDefinition('name', true),
        );

        static::assertNull($entry->value());
        static::assertTrue($entry->definition()->isNullable());
    }
}
