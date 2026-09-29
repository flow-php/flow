<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use ArrayObject;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowEquality;
use Flow\ETL\Tests\Double\CountingDOMDocument;
use Flow\ETL\Tests\Double\FixedValuesColumn;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_xml;
use function range;

final class RowEqualityTest extends FlowTestCase
{
    public function test_equal_skips_a_column_one_side_lacks(): void
    {
        static::assertTrue((new RowEquality())->equal(
            ['id' => type_integer(), 'name' => type_string()],
            ['id' => [1]],
            0,
            ['id' => [1], 'name' => ['a']],
            0,
        ));
    }

    public function test_not_in_keeps_rows_without_an_equal_row_in_the_other_batch(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertSame(
            [1, 2],
            (new RowEquality())->notIn(
                array_to_rows([
                    ['id' => 1, 'name' => 'a'],
                    ['id' => 2, 'name' => 'b'],
                    ['id' => 1, 'name' => 'z'],
                ], $schema),
                array_to_rows([['id' => 1, 'name' => 'a'], ['id' => 3, 'name' => 'c']], $schema),
                $schema,
            ),
        );
    }

    public function test_not_in_keeps_every_row_when_the_columns_differ(): void
    {
        static::assertSame(
            [0, 1],
            (new RowEquality())->notIn(
                array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
                array_to_rows([['id' => 1, 'name' => 'a']], schema(int_schema('id'), str_schema('name'))),
                schema(int_schema('id')),
            ),
        );
    }

    public function test_unique_compares_values_not_instances(): void
    {
        $schema = schema(uuid_schema('id'));

        static::assertSame(
            [0, 2],
            (new RowEquality())->unique(array_to_rows([
                ['id' => new Uuid('0198f4f2-37e3-7e1c-9a2e-1f2b3c4d5e6f')],
                ['id' => new Uuid('0198f4f2-37e3-7e1c-9a2e-1f2b3c4d5e6f')],
                ['id' => new Uuid('0198f4f2-37e3-7e1c-9a2e-1f2b3c4d5e70')],
            ], $schema)),
        );
    }

    public function test_unique_compares_only_rows_whose_exact_columns_hash_alike(): void
    {
        $calls = new ArrayObject();
        $documents = [];

        for ($i = 0; $i < 20; $i++) {
            $documents[] = new CountingDOMDocument($calls);
        }

        $ids = array_to_rows(
            array_map(static fn(int $id): array => ['id' => $id], range(0, 19)),
            schema(int_schema('id')),
        );
        $schema = schema(xml_schema('x'), int_schema('id'));
        $rows = Rows::fromColumns(
            $schema,
            ['x' => new FixedValuesColumn(type_xml(), $documents), 'id' => $ids->column('id')],
            20,
        );

        static::assertSame(range(0, 19), (new RowEquality())->unique($rows));
        static::assertCount(0, $calls);
    }

    public function test_not_in_compares_only_rows_whose_exact_columns_hash_alike(): void
    {
        $calls = new ArrayObject();
        $schema = schema(xml_schema('x'), int_schema('id'));
        $batch = static fn(int $from): Rows => Rows::fromColumns(
            $schema,
            [
                'x' => new FixedValuesColumn(type_xml(), array_map(
                    static fn(): CountingDOMDocument => new CountingDOMDocument($calls),
                    range(0, 9),
                )),
                'id' => array_to_rows(
                    array_map(static fn(int $id): array => ['id' => $id], range($from, $from + 9)),
                    schema(int_schema('id')),
                )->column('id'),
            ],
            10,
        );

        static::assertSame(range(0, 9), (new RowEquality())->notIn($batch(0), $batch(100), $schema));
        static::assertCount(0, $calls);
    }

    public function test_unique_keeps_json_rows_equal_by_value_although_they_hash_apart(): void
    {
        static::assertSame(
            [0],
            (new RowEquality())->unique(array_to_rows([
                ['j' => new Json('{"a":1,"b":2}')],
                ['j' => new Json('{"b":2,"a":1}')],
            ], schema(json_schema('j')))),
        );
    }

    public function test_unique_keeps_the_first_row_of_every_equal_group(): void
    {
        static::assertSame(
            [0, 1, 4],
            (new RowEquality())->unique(array_to_rows(
                [
                    ['id' => 1, 'name' => 'a'],
                    ['id' => 2, 'name' => 'b'],
                    ['id' => 1, 'name' => 'a'],
                    ['id' => 2, 'name' => 'b'],
                    ['id' => 3, 'name' => null],
                ],
                schema(int_schema('id'), str_schema('name', nullable: true)),
            )),
        );
    }

    public function test_unique_of_an_empty_batch_is_empty(): void
    {
        static::assertSame([], (new RowEquality())->unique(array_to_rows([], schema(int_schema('id')))));
    }
}
