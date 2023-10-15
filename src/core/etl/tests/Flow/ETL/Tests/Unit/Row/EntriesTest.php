<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use PHPUnit\Framework\TestCase;

final class EntriesTest extends TestCase
{
    public function test_add_entry() : void
    {
        $newEntry = new StringEntry('entry-name', 'new string entry');
        $entries = new Entries(
            new IntegerEntry('integer-entry', 100)
        );
        $this->assertFalse($entries->has('entry-name'));

        $entries = $entries->add($newEntry);

        $this->assertTrue($entries->has('entry-name'));
        $this->assertEquals($newEntry, $entries->get('entry-name'));
    }

    public function test_add_multiple_duplicated_entries() : void
    {
        $stringEntry = new StringEntry('string-name', 'new string entry');
        $booleanEntry = new StringEntry('string-name', 'new string entry');

        $entries = new Entries(new IntegerEntry('integer-entry', 100));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Added entries names must be unique, given: [integer-entry, string-name] + [string-name]');

        $entries->add($stringEntry)->add($booleanEntry);
    }

    public function test_add_multiple_entries() : void
    {
        $stringEntry = new StringEntry('string-name', 'new string entry');
        $booleanEntry = new BooleanEntry('boolean-name', true);

        $entries = new Entries(new IntegerEntry('integer-entry', 100));

        $this->assertFalse($entries->has('string-name'));
        $this->assertFalse($entries->has('boolean-name'));

        $entries = $entries->add($stringEntry)->add($booleanEntry);

        $this->assertTrue($entries->has('string-name'));
        $this->assertTrue($entries->has('boolean-name'));
        $this->assertEquals($stringEntry, $entries->get('string-name'));
        $this->assertEquals($booleanEntry, $entries->get('boolean-name'));
    }

    public function test_adds_entry_when_it_does_not_exist() : void
    {
        $stringEntry = new StringEntry('string-entry', 'just a string');
        $entries = new Entries(
            $integerEntry = new IntegerEntry('integer-entry', 100),
            $booleanEntry = new BooleanEntry('boolean-entry', true)
        );

        $entries = $entries->set($stringEntry);

        $this->assertEquals(new Entries($integerEntry, $booleanEntry, $stringEntry), $entries);
    }

    public function test_array_access_exists() : void
    {
        $entries = new Entries(new IntegerEntry('id', 1), new StringEntry('name', 'John'));

        $this->assertTrue(isset($entries['id']));
        $this->assertFalse(isset($entries['test']));
    }

    public function test_array_access_get() : void
    {
        $entries = new Entries(new IntegerEntry('id', 1), new StringEntry('name', 'John'));

        $this->assertSame(1, $entries['id']->value());
        $this->assertSame('John', $entries['name']->value());
    }

    public function test_array_access_set() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Entries::add(Entry $entry) : self');
        $entries = new Entries();
        $entries['id'] = new IntegerEntry('id', 1);
    }

    public function test_array_access_unset() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Entries::remove(string $name) : self');
        $entries = new Entries(new IntegerEntry('id', 1));
        unset($entries['id']);
    }

    public function test_assert_if_entry_exists_when_removing_entry() : void
    {
        $entries = new Entries(
            new IntegerEntry('integer-entry', 100),
            new StringEntry('string-entry', 'just a string'),
        );

        $this->expectExceptionMessage('Entry "non-existing-entry" does not exist');

        $entries->remove('non-existing-entry');
    }

    public function test_case_sensitive_entry_names() : void
    {
        $entries = new Entries(
            new StringEntry('entry-Name', 'just a string'),
        );

        $this->assertFalse($entries->has('entry-name'));
    }

    public function test_create_from_non_unique_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry names must be unique, given: [integer-entry, integer-entry]');

        new Entries(
            new IntegerEntry('integer-entry', 100),
            new IntegerEntry('integer-entry', 200)
        );
    }

    public function test_get_all_entries() : void
    {
        $entries = new Entries(
            Entry::integer('id', 1),
            Entry::integer('name', 1),
        );

        $this->assertCount(
            2,
            $entries->getAll('id', 'name')
        );
    }

    public function test_get_all_entries_when_at_least_one_is_missing() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $entries = new Entries(
            Entry::integer('id', 1),
            Entry::integer('name', 1),
        );

        $entries->getAll('id', 'name', 'status');
    }

    public function test_has_when_at_least_one_is_missing() : void
    {
        $entries = new Entries(
            Entry::integer('id', 1),
            Entry::integer('name', 1),
        );

        $this->assertFalse($entries->has('id', 'name', 'status'));
    }

    public function test_has_when_none_of_many_is_missing() : void
    {
        $entries = new Entries(
            Entry::integer('id', 1),
            Entry::integer('name', 1),
            Entry::boolean('active', true)
        );

        $this->assertTrue($entries->has('id', 'name'));
    }

    public function test_merge_duplicated_entries() : void
    {
        $entries1 = new Entries(new StringEntry('string-name', 'new string entry'));
        $entries2 = new Entries(new StringEntry('string-name', 'new string entry'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [string-name] + [string-name]');

        $entries1->merge($entries2);
    }

    public function test_merge_duplicated_entries_case_insensitive_() : void
    {
        $entries1 = new Entries(new StringEntry('string-name', 'new string entry'));
        $entries2 = new Entries(new StringEntry('string-Name', 'new string entry'));

        $merged = $entries1->merge($entries2);

        $this->assertCount(2, $merged);
    }

    public function test_merge_entries() : void
    {
        $entries1 = new Entries(new StringEntry('string-name', 'new string entry'));
        $entries2 = new Entries(new IntegerEntry('integer-entry', 100));

        $entries = $entries1->merge($entries2);

        $this->assertEquals(
            new Entries(new StringEntry('string-name', 'new string entry'), new IntegerEntry('integer-entry', 100)),
            $entries
        );
    }

    public function test_order_entries() : void
    {
        $entries = new Entries(
            new IntegerEntry('integer', 100),
            new StringEntry('string', 'new string entry'),
            new BooleanEntry('bool', true),
        );

        $this->assertEquals(
            ['integer', 'string', 'bool'],
            $entries->map(fn (\Flow\ETL\Row\Entry $e) => $e->name())
        );

        $entries = $entries->order('bool', 'string', 'integer');

        $this->assertEquals(
            ['bool', 'string', 'integer'],
            $entries->map(fn (\Flow\ETL\Row\Entry $e) => $e->name())
        );
    }

    public function test_order_entries_without_providing_all_entry_names() : void
    {
        $this->expectExceptionMessage('In order to sort entries in a given order you need to provide all entry names, given: "bool", "string", expected: "integer", "string", "bool"');

        $entries = new Entries(
            new IntegerEntry('integer', 100),
            new StringEntry('string', 'new string entry'),
            new BooleanEntry('bool', true),
        );

        $entries->order('bool', 'string');
    }

    public function test_overwrites_entry_when_it_exists() : void
    {
        $stringEntry = new StringEntry('entry-name', 'just a string');
        $entries = new Entries(
            new IntegerEntry('entry-name', 100),
            $booleanEntry = new BooleanEntry('boolean-entry', true)
        );

        $entries = $entries->set($stringEntry);

        $this->assertEquals(new Entries($booleanEntry, $stringEntry), $entries);
    }

    public function test_prevents_from_adding_entry_with_the_same_name() : void
    {
        $entries = new Entries(
            new IntegerEntry('entry-name', 100)
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Added entries names must be unique, given: [entry-name] + [entry-name]');

        $entries->add(new StringEntry('entry-name', 'just a string'));
    }

    public function test_prevents_from_adding_entry_with_the_same_name_case_insensitive() : void
    {
        $entries = new Entries(
            new IntegerEntry('entry-Name', 100)
        );

        $newEntries = $entries->add(new StringEntry('entry-name', 'just a string'));

        $this->assertCount(2, $newEntries);
    }

    public function test_prevents_from_creating_collection_with_duplicate_entry_names() : void
    {
        $this->expectExceptionMessage('Entry names must be unique');

        new Entries(
            new StringEntry('entry-name', 'just a string'),
            new IntegerEntry('entry-name', 100)
        );
    }

    public function test_prevents_from_getting_unknown_entry() : void
    {
        $entries = new Entries();

        $this->expectExceptionMessage('Entry "unknown" does not exist');

        $entries->get('unknown');
    }

    public function test_remove_entry() : void
    {
        $entries = new Entries(
            $integerEntry = new IntegerEntry('integer-entry', 100),
            new StringEntry('string-entry', 'just a string'),
            $booleanEntry = new BooleanEntry('boolean-entry', true)
        );

        $this->assertEquals(new Entries($integerEntry, $booleanEntry), $entries->remove('string-entry'));
    }

    public function test_remove_multiple_entries() : void
    {
        $entries = new Entries(
            new IntegerEntry('integer-entry', 100),
            new StringEntry('string-entry', 'just a string'),
            $booleanEntry = new BooleanEntry('boolean-entry', true)
        );

        $this->assertEquals(new Entries($booleanEntry), $entries->remove('string-entry', 'integer-entry'));
    }

    public function test_rename() : void
    {
        $entries = new Entries(new StringEntry('string-name', 'new string entry'));

        $entries = $entries->rename('string-name', 'new-string-name');

        $this->assertEquals(
            new Entries(new StringEntry('new-string-name', 'new string entry')),
            $entries
        );
    }

    public function test_set_entry() : void
    {
        $entries = new Entries(new StringEntry('string-entry', 'just a string'));
        $entries = $entries->set(new StringEntry('string-entry', 'new string'));

        $this->assertEquals(new Entries(new StringEntry('string-entry', 'new string')), $entries);
    }

    public function test_set_multiple_entries() : void
    {
        $entries = new Entries(new StringEntry('string-entry', 'just a string'));
        $entries = $entries->set(new StringEntry('string-entry', 'new string'), new IntegerEntry('integer-entry', 100));

        $this->assertEquals(new Entries(new StringEntry('string-entry', 'new string'), new IntegerEntry('integer-entry', 100)), $entries);
    }

    public function test_sorts_entries_by_name() : void
    {
        $entries = new Entries(
            $id = new IntegerEntry('id', 1234),
            $deleted = new BooleanEntry('deleted', false),
            $createdAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
            $phase = new NullEntry('phase'),
            $items = new StructureEntry(
                'items',
                new IntegerEntry('item-id', 1),
                new StringEntry('name', 'one'),
            )
        );

        $sorted = $entries->sort();

        $this->assertEquals(
            new Entries(
                $createdAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
                $deleted = new BooleanEntry('deleted', false),
                $id = new IntegerEntry('id', 1234),
                $items = new StructureEntry(
                    'items',
                    new IntegerEntry('item-id', 1),
                    new StringEntry('name', 'one'),
                ),
                $phase = new NullEntry('phase')
            ),
            $sorted
        );
    }

    public function test_transforms_collection_to_array() : void
    {
        $entries = new Entries(
            new IntegerEntry('id', 1234),
            new BooleanEntry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
            new NullEntry('phase'),
            new StructureEntry(
                'items',
                new IntegerEntry('item-id', 1),
                new StringEntry('name', 'one'),
            ),
            new EnumEntry('enum', BasicEnum::three)
        );

        $this->assertEquals(
            [
                'id' => 1234,
                'deleted' => false,
                'created-at' => $createdAt,
                'phase' => null,
                'items' => [
                    'item-id' => 1,
                    'name' => 'one',
                ],
                'enum' => BasicEnum::three,
            ],
            $entries->toArray()
        );
    }
}
