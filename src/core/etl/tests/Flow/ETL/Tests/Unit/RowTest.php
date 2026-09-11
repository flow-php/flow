<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Exception\ColumnMismatchException;
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
    public function test_conform_to_does_not_validate_a_non_null_value(): void
    {
        // the caller produced the value by casting to the column's type - conformTo() checks the shape only
        static::assertSame(
            ['a' => 'not-an-integer'],
            row(['a' => 'not-an-integer'])->conformTo(schema(int_schema('a')))->values(),
        );
    }

    public function test_conform_to_carries_the_schema_order_and_pads_a_nullable_absence(): void
    {
        static::assertSame(
            ['c' => 3, 'a' => 1, 'b' => null],
            row(['a' => 1, 'c' => 3])->conformTo(schema(
                int_schema('c'),
                int_schema('a'),
                str_schema('b', true),
            ))->values(),
        );
    }

    public function test_conform_to_refuses_a_null_under_not_null(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "a"');

        row(['a' => null])->conformTo(schema(int_schema('a')));
    }

    public function test_conform_to_refuses_a_missing_not_null_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "b"');

        row(['a' => 1])->conformTo(schema(int_schema('a'), int_schema('b')));
    }

    public function test_conform_to_refuses_an_unknown_column(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "z"');

        row(['a' => 1, 'z' => 2])->conformTo(schema(int_schema('a')));
    }

    public function test_match_to_accepts_null_in_a_nullable_column(): void
    {
        static::assertSame(['a' => null], row(['a' => null])->matchTo(schema(str_schema('a', true)))->values());
    }

    public function test_match_to_carries_the_schema_order_into_the_row(): void
    {
        static::assertSame(
            ['c', 'a', 'b'],
            row(['a' => 1, 'b' => 2, 'c' => 3])->matchTo(schema(
                int_schema('c'),
                int_schema('a'),
                int_schema('b'),
            ))->names(),
        );
    }

    public function test_match_to_leaves_a_matching_row_unchanged(): void
    {
        static::assertSame(['a' => 1], row(['a' => 1])->matchTo(schema(int_schema('a')))->values());
    }

    public function test_match_to_pads_a_declared_nullable_column_the_row_omits(): void
    {
        static::assertSame(
            ['a' => 1, 'b' => null],
            row(['a' => 1])->matchTo(schema(int_schema('a'), str_schema('b', true)))->values(),
        );
    }

    public function test_match_to_rejects_a_column_the_schema_does_not_declare(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('Row does not match its schema: column "b" is not declared by the schema');

        row(['a' => 1, 'b' => 2])->matchTo(schema(int_schema('a')));
    }

    public function test_match_to_rejects_a_row_against_an_empty_schema(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('Row does not match its schema: column "a" is not declared by the schema');

        row(['a' => 1])->matchTo(schema());
    }

    public function test_match_to_rejects_a_value_of_the_wrong_type(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(
            'Row does not match its schema: column "code": could not convert 1000 (integer) to string',
        );

        row(['code' => 1000])->matchTo(schema(str_schema('code')));
    }

    public function test_match_to_rejects_null_in_a_column_that_is_not_nullable(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('column "a": could not convert null to string, column is not nullable');

        row(['a' => null])->matchTo(schema(str_schema('a')));
    }

    public function test_match_to_rejects_when_a_declared_column_that_is_not_nullable_is_missing(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(
            'Row does not match its schema: column "b" declared by the schema is missing from the row',
        );

        row(['a' => 1])->matchTo(schema(int_schema('a'), int_schema('b')));
    }

    public function test_match_to_reports_no_row_coordinate_of_its_own(): void
    {
        // the row does not know its position - Rows attaches it, see RowsTest
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage('Row does not match its schema: column "a" is not declared by the schema');

        row(['a' => 1])->matchTo(schema());
    }

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

    public function test_project_carries_the_schema_order_into_the_row(): void
    {
        static::assertSame(
            ['c', 'a'],
            row(['a' => 1, 'c' => 3])->project(schema(int_schema('c'), int_schema('a')))->names(),
        );
    }

    public function test_project_drops_the_columns_the_schema_does_not_declare(): void
    {
        static::assertSame(['a' => 1], row(['a' => 1, 'b' => 2])->project(schema(int_schema('a')))->values());
    }

    public function test_project_leaves_a_column_the_row_omits_absent_instead_of_padding_it(): void
    {
        static::assertSame(
            ['a' => 1],
            row(['a' => 1])->project(schema(int_schema('a'), str_schema('b', true)))->values(),
        );
    }

    public function test_project_validates_nothing(): void
    {
        static::assertSame(['code' => 1000], row(['code' => 1000])->project(schema(str_schema('code')))->values());
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
