<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\{bool_entry, int_entry, string_entry, type_int, type_string};
use function Flow\ETL\DSL\{boolean_entry, enum_entry, integer_entry, structure_element, structure_entry, type_structure};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Row\Entry\{DateTimeEntry};
use Flow\ETL\Row\{Entries, Entry};
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\ETL\Tests\FlowTestCase;

final class EntriesTest extends FlowTestCase
{
    public function test_add_entry() : void
    {
        $newEntry = string_entry('entry-name', 'new string entry');
        $entries = new Entries(
            integer_entry('integer-entry', 100)
        );
        self::assertFalse($entries->has('entry-name'));

        $entries = $entries->add($newEntry);

        self::assertTrue($entries->has('entry-name'));
        self::assertEquals($newEntry, $entries->get('entry-name'));
    }

    public function test_add_multiple_duplicated_entries() : void
    {
        $stringEntry = string_entry('string-name', 'new string entry');
        $booleanEntry = string_entry('string-name', 'new string entry');

        $entries = new Entries(integer_entry('integer-entry', 100));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Added entries names must be unique, given: [integer-entry, string-name] + [string-name]');

        $entries->add($stringEntry)->add($booleanEntry);
    }

    public function test_add_multiple_entries() : void
    {
        $stringEntry = string_entry('string-name', 'new string entry');
        $booleanEntry = boolean_entry('boolean-name', true);

        $entries = new Entries(integer_entry('integer-entry', 100));

        self::assertFalse($entries->has('string-name'));
        self::assertFalse($entries->has('boolean-name'));

        $entries = $entries->add($stringEntry)->add($booleanEntry);

        self::assertTrue($entries->has('string-name'));
        self::assertTrue($entries->has('boolean-name'));
        self::assertEquals($stringEntry, $entries->get('string-name'));
        self::assertEquals($booleanEntry, $entries->get('boolean-name'));
    }

    public function test_adds_entry_when_it_does_not_exist() : void
    {
        $stringEntry = string_entry('string-entry', 'just a string');
        $entries = new Entries(
            $integerEntry = integer_entry('integer-entry', 100),
            $booleanEntry = boolean_entry('boolean-entry', true)
        );

        $entries = $entries->set($stringEntry);

        self::assertEquals(new Entries($integerEntry, $booleanEntry, $stringEntry), $entries);
    }

    public function test_array_access_exists() : void
    {
        $entries = new Entries(integer_entry('id', 1), string_entry('name', 'John'));

        self::assertTrue(isset($entries['id']));
        self::assertFalse(isset($entries['test']));
    }

    public function test_array_access_get() : void
    {
        $entries = new Entries(integer_entry('id', 1), string_entry('name', 'John'));

        self::assertSame(1, $entries['id']->value());
        self::assertSame('John', $entries['name']->value());
    }

    public function test_array_access_set() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Entries::add(Entry $entry) : self');
        $entries = new Entries();
        $entries['id'] = integer_entry('id', 1);
    }

    public function test_array_access_unset() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('In order to add new rows use Entries::remove(string $name) : self');
        $entries = new Entries(integer_entry('id', 1));
        unset($entries['id']);
    }

    public function test_assert_if_entry_exists_when_removing_entry() : void
    {
        $entries = new Entries(
            integer_entry('integer-entry', 100),
            string_entry('string-entry', 'just a string'),
        );

        $this->expectExceptionMessage('Entry "non-existing-entry" does not exist');

        $entries->remove('non-existing-entry');
    }

    public function test_case_sensitive_entry_names() : void
    {
        $entries = new Entries(
            string_entry('entry-Name', 'just a string'),
        );

        self::assertFalse($entries->has('entry-name'));
    }

    public function test_create_from_non_unique_entries() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry names must be unique, given: [integer-entry, integer-entry]');

        new Entries(
            integer_entry('integer-entry', 100),
            integer_entry('integer-entry', 200)
        );
    }

    public function test_get_all_entries() : void
    {
        $entries = new Entries(
            int_entry('id', 1),
            int_entry('name', 1),
        );

        self::assertCount(
            2,
            $entries->getAll('id', 'name')
        );
    }

    public function test_get_all_entries_when_at_least_one_is_missing() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $entries = new Entries(
            int_entry('id', 1),
            int_entry('name', 1),
        );

        $entries->getAll('id', 'name', 'status');
    }

    public function test_has_when_at_least_one_is_missing() : void
    {
        $entries = new Entries(
            int_entry('id', 1),
            int_entry('name', 1),
        );

        self::assertFalse($entries->has('id', 'name', 'status'));
    }

    public function test_has_when_none_of_many_is_missing() : void
    {
        $entries = new Entries(
            int_entry('id', 1),
            int_entry('name', 1),
            bool_entry('active', true)
        );

        self::assertTrue($entries->has('id', 'name'));
    }

    public function test_merge_duplicated_entries() : void
    {
        $entries1 = new Entries(string_entry('string-name', 'new string entry'));
        $entries2 = new Entries(string_entry('string-name', 'new string entry'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merged entries names must be unique, given: [string-name] + [string-name]');

        $entries1->merge($entries2);
    }

    public function test_merge_duplicated_entries_case_insensitive_() : void
    {
        $entries1 = new Entries(string_entry('string-name', 'new string entry'));
        $entries2 = new Entries(string_entry('string-Name', 'new string entry'));

        $merged = $entries1->merge($entries2);

        self::assertCount(2, $merged);
    }

    public function test_merge_entries() : void
    {
        $entries1 = new Entries(string_entry('string-name', 'new string entry'));
        $entries2 = new Entries(integer_entry('integer-entry', 100));

        $entries = $entries1->merge($entries2);

        self::assertEquals(
            new Entries(string_entry('string-name', 'new string entry'), integer_entry('integer-entry', 100)),
            $entries
        );
    }

    public function test_order_entries() : void
    {
        $entries = new Entries(
            integer_entry('integer', 100),
            string_entry('string', 'new string entry'),
            boolean_entry('bool', true),
        );

        self::assertEquals(
            ['integer', 'string', 'bool'],
            $entries->map(fn (Entry $e) => $e->name())
        );

        $entries = $entries->order('bool', 'string', 'integer');

        self::assertEquals(
            ['bool', 'string', 'integer'],
            $entries->map(fn (Entry $e) => $e->name())
        );
    }

    public function test_order_entries_without_providing_all_entry_names() : void
    {
        $this->expectExceptionMessage('In order to sort entries in a given order you need to provide all entry names, given: "bool", "string", expected: "integer", "string", "bool"');

        $entries = new Entries(
            integer_entry('integer', 100),
            string_entry('string', 'new string entry'),
            boolean_entry('bool', true),
        );

        $entries->order('bool', 'string');
    }

    public function test_overwrites_entry_when_it_exists() : void
    {
        $stringEntry = string_entry('entry-name', 'just a string');
        $entries = new Entries(
            integer_entry('entry-name', 100),
            $booleanEntry = boolean_entry('boolean-entry', true)
        );

        $entries = $entries->set($stringEntry);

        self::assertEquals(new Entries($booleanEntry, $stringEntry), $entries);
    }

    public function test_prevents_from_adding_entry_with_the_same_name() : void
    {
        $entries = new Entries(
            integer_entry('entry-name', 100)
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Added entries names must be unique, given: [entry-name] + [entry-name]');

        $entries->add(string_entry('entry-name', 'just a string'));
    }

    public function test_prevents_from_adding_entry_with_the_same_name_case_insensitive() : void
    {
        $entries = new Entries(
            integer_entry('entry-Name', 100)
        );

        $newEntries = $entries->add(string_entry('entry-name', 'just a string'));

        self::assertCount(2, $newEntries);
    }

    public function test_prevents_from_creating_collection_with_duplicate_entry_names() : void
    {
        $this->expectExceptionMessage('Entry names must be unique');

        new Entries(
            string_entry('entry-name', 'just a string'),
            integer_entry('entry-name', 100)
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
            $integerEntry = integer_entry('integer-entry', 100),
            string_entry('string-entry', 'just a string'),
            $booleanEntry = boolean_entry('boolean-entry', true)
        );

        self::assertEquals(new Entries($integerEntry, $booleanEntry), $entries->remove('string-entry'));
    }

    public function test_remove_multiple_entries() : void
    {
        $entries = new Entries(
            integer_entry('integer-entry', 100),
            string_entry('string-entry', 'just a string'),
            $booleanEntry = boolean_entry('boolean-entry', true)
        );

        self::assertEquals(new Entries($booleanEntry), $entries->remove('string-entry', 'integer-entry'));
    }

    public function test_rename() : void
    {
        $entries = new Entries(string_entry('string-name', 'new string entry'));

        $entries = $entries->rename('string-name', 'new-string-name');

        self::assertEquals(
            new Entries(string_entry('new-string-name', 'new string entry')),
            $entries
        );
    }

    public function test_set_entry() : void
    {
        $entries = new Entries(string_entry('string-entry', 'just a string'));
        $entries = $entries->set(string_entry('string-entry', 'new string'));

        self::assertEquals(new Entries(string_entry('string-entry', 'new string')), $entries);
    }

    public function test_set_multiple_entries() : void
    {
        $entries = new Entries(string_entry('string-entry', 'just a string'));
        $entries = $entries->set(string_entry('string-entry', 'new string'), integer_entry('integer-entry', 100));

        self::assertEquals(new Entries(string_entry('string-entry', 'new string'), integer_entry('integer-entry', 100)), $entries);
    }

    public function test_sorts_entries_by_name() : void
    {
        $entries = new Entries(
            $id = integer_entry('id', 1234),
            $deleted = boolean_entry('deleted', false),
            $createdAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
            $phase = string_entry('phase', null),
            $items = structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([structure_element('id', type_int()), structure_element('name', type_string())]))
        );

        $sorted = $entries->sort();

        self::assertEquals(
            new Entries(
                $createdAt = new DateTimeEntry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
                $deleted = boolean_entry('deleted', false),
                $id = integer_entry('id', 1234),
                $items = structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([structure_element('id', type_int()), structure_element('name', type_string())])),
                $phase = string_entry('phase', null)
            ),
            $sorted
        );
    }

    public function test_transforms_collection_to_array() : void
    {
        $entries = new Entries(
            integer_entry('id', 1234),
            boolean_entry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
            string_entry('phase', null),
            structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([structure_element('id', type_int()), structure_element('name', type_string())])),
            enum_entry('enum', BasicEnum::three)
        );

        self::assertEquals(
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

    public function test_transforms_collection_to_array_without_keys() : void
    {
        $entries = new Entries(
            integer_entry('id', 1234),
            boolean_entry('deleted', false),
            new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
            string_entry('phase', null),
            structure_entry('items', ['item-id' => 1, 'name' => 'one'], type_structure([structure_element('id', type_int()), structure_element('name', type_string())])),
            enum_entry('enum', BasicEnum::three)
        );

        self::assertEquals(
            [
                1234,
                false,
                $createdAt,
                null,
                [
                    'item-id' => 1,
                    'name' => 'one',
                ],
                BasicEnum::three,
            ],
            $entries->toArray(withKeys: false)
        );
    }
}
