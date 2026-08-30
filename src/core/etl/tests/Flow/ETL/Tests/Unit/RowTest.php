<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class RowTest extends FlowTestCase
{
    public function test_get_throws_when_the_column_is_absent(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "missing" does not exist');

        row(['id' => 1])->get('missing');
    }

    public function test_has(): void
    {
        $row = row(['id' => 1, 'name' => 'one']);

        static::assertTrue($row->has('id'));
        static::assertTrue($row->has('id', 'name'));
        static::assertFalse($row->has('missing'));
        static::assertFalse($row->has('id', 'missing'));
    }

    /**
     * The hash is a dedup key, so it stays order-independent: the Schema is sorted alphabetically
     * before the values are folded, exactly as Entries::sort() did.
     */
    public function test_hash_ignores_column_order(): void
    {
        $schema = schema(int_schema('id'), bool_schema('bool'), str_schema('string'));

        static::assertSame(
            row(['id' => 1, 'string' => 'string', 'bool' => false])->hash($schema),
            row(['bool' => false, 'id' => 1, 'string' => 'string'])->hash($schema),
        );
    }

    public function test_hash_of_an_empty_row(): void
    {
        static::assertSame(row([])->hash(schema()), row([])->hash(schema()));
    }

    public function test_hash_of_different_rows(): void
    {
        $schema = schema(list_schema('list', type_list(type_integer())));

        static::assertNotSame(row(['list' => [1, 2, 3]])->hash($schema), row(['list' => [3, 2, 1]])->hash($schema));
    }

    public function test_names_and_values_keep_storage_order(): void
    {
        $row = row(['b' => 2, 'a' => 1]);

        static::assertSame(['b', 'a'], $row->names());
        static::assertSame(['b' => 2, 'a' => 1], $row->values());
    }

    public function test_transforms_row_to_array(): void
    {
        $row = row([
            'id' => 1234,
            'deleted' => false,
            'created-at' => $createdAt = new DateTimeImmutable('2020-07-13 15:00'),
            'phase' => null,
            'items' => ['item-id' => 1, 'name' => 'one'],
            'statuses' => ['NEW', 'PENDING'],
        ]);

        static::assertEquals(
            [
                'id' => 1234,
                'deleted' => false,
                'created-at' => $createdAt,
                'phase' => null,
                'items' => ['item-id' => 1, 'name' => 'one'],
                'statuses' => ['NEW', 'PENDING'],
            ],
            $row->toArray(),
        );
    }

    public function test_transforms_row_to_array_without_keys(): void
    {
        static::assertSame([1, 'one'], row(['id' => 1, 'name' => 'one'])->toArray(withKeys: false));
    }

    /**
     * The Schema names, types and orders the columns; the Row is name-keyed storage.
     */
    public function test_values_are_read_by_the_schema(): void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('name'),
            datetime_schema('created-at'),
            structure_schema('items', type_structure(['a' => type_integer()])),
            map_schema('statuses', type_map(type_integer(), type_string())),
        );
        $createdAt = new DateTimeImmutable('2020-07-13 15:00');
        $row = row([
            'id' => 1,
            'name' => 'one',
            'created-at' => $createdAt,
            'items' => ['a' => 1],
            'statuses' => ['NEW'],
        ]);

        $values = [];

        foreach ($schema->references()->names() as $name) {
            $values[$name] = $row->get($name);
        }

        static::assertSame(
            [
                'id' => 1,
                'name' => 'one',
                'created-at' => $createdAt,
                'items' => ['a' => 1],
                'statuses' => ['NEW'],
            ],
            $values,
        );
    }
}
