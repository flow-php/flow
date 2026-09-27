<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
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

        array_to_row(['id' => 1], schema(int_schema('id')))->get('missing');
    }

    #[TestWith([-1])]
    #[TestWith([1])]
    public function test_a_view_outside_its_batch_is_refused(int $index): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Row {$index} does not exist in a batch of 1 rows");

        array_to_row(['id' => 1], schema(int_schema('id')))->rows->row($index);
    }

    public function test_has(): void
    {
        $row = array_to_row(['id' => 1, 'name' => 'one'], schema(int_schema('id'), str_schema('name')));

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
            array_to_row(
                ['id' => 1, 'string' => 'string', 'bool' => false],
                schema(int_schema('id'), str_schema('string'), bool_schema('bool')),
            )->hash($schema),
            array_to_row(
                ['bool' => false, 'id' => 1, 'string' => 'string'],
                schema(bool_schema('bool'), int_schema('id'), str_schema('string')),
            )->hash($schema),
        );
    }

    public function test_hash_of_an_empty_row(): void
    {
        static::assertSame(array_to_row([], schema())->hash(schema()), array_to_row([], schema())->hash(schema()));
    }

    public function test_hash_of_different_rows(): void
    {
        $schema = schema(list_schema('list', type_list(type_integer())));

        static::assertNotSame(
            array_to_row(['list' => [1, 2, 3]], schema(list_schema('list', type_list(type_integer()))))->hash($schema),
            array_to_row(['list' => [3, 2, 1]], schema(list_schema('list', type_list(type_integer()))))->hash($schema),
        );
    }

    public function test_names_and_values_keep_storage_order(): void
    {
        $row = array_to_row(['b' => 2, 'a' => 1], schema(int_schema('b'), int_schema('a')));

        static::assertSame(['b', 'a'], $row->names());
        static::assertSame(['b' => 2, 'a' => 1], $row->values());
    }

    public function test_transforms_row_to_array(): void
    {
        $row = array_to_row(
            [
                'id' => 1234,
                'deleted' => false,
                'created-at' => $createdAt = new DateTimeImmutable('2020-07-13 15:00'),
                'phase' => null,
                'items' => ['item-id' => 1, 'name' => 'one'],
                'statuses' => ['NEW', 'PENDING'],
            ],
            schema(
                int_schema('id'),
                bool_schema('deleted'),
                datetime_schema('created-at'),
                str_schema('phase', nullable: true),
                structure_schema('items', type_structure(['item-id' => type_integer(), 'name' => type_string()])),
                list_schema('statuses', type_list(type_string())),
            ),
        );

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
        static::assertSame(
            [1, 'one'],
            array_to_row(['id' => 1, 'name' => 'one'], schema(int_schema('id'), str_schema('name')))->toArray(
                withKeys: false,
            ),
        );
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
        $row = array_to_row([
            'id' => 1,
            'name' => 'one',
            'created-at' => $createdAt,
            'items' => ['a' => 1],
            'statuses' => ['NEW'],
        ], $schema);

        $values = [];

        foreach ($schema->references()->names() as $name) {
            $values[$name] = $row->get($name);
        }

        static::assertEquals(
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
