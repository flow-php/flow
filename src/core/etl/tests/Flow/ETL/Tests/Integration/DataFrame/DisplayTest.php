<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_xml;
use function ob_get_clean;
use function ob_start;

final class DisplayTest extends FlowIntegrationTestCase
{
    use CommandOutputNormalizer;

    public function test_display(): void
    {
        $etl = df()->read(new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return new Schema();
            }

            /**
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context): Generator
            {
                for ($i = 0; $i < 20; $i++) {
                    yield rows(
                        schema(
                            int_schema('id'),
                            float_schema('price'),
                            int_schema('100'),
                            bool_schema('deleted'),
                            datetime_schema('created-at'),
                            str_schema('phase', nullable: true),
                            json_schema('array'),
                            list_schema('list', type_list(type_integer())),
                            map_schema('map', type_map(type_integer(), type_string())),
                            structure_schema('items', type_structure([
                                'item-id' => type_string(),
                                'name' => type_string(),
                            ])),
                            enum_schema('enum', BackedStringEnum::class),
                            xml_schema('xml'),
                        ),
                        // PHP casts the numeric column name "100" to an int array key
                        // @mago-ignore analysis:possibly-invalid-argument
                        row([
                            'id' => 1234,
                            'price' => 123.45,
                            '100' => 100,
                            'deleted' => false,
                            'created-at' => new DateTimeImmutable('2020-07-13 15:00'),
                            'phase' => null,
                            'array' => type_json()->cast([
                                ['id' => 1, 'status' => 'NEW'],
                                ['id' => 2, 'status' => 'PENDING'],
                            ]),
                            'list' => [1, 2, 3],
                            'map' => ['NEW', 'PENDING'],
                            'items' => ['item-id' => '1', 'name' => 'one'],
                            'enum' => BackedStringEnum::three,
                            'xml' => type_xml()->cast('<xml><node id="123">test<foo>bar</foo></node></xml>'),
                        ]),
                    );
                }
            }
        })->collect();

        self::assertCommandOutputIdentical(<<<'ASCIITABLE'
            +------+------------+-----+---------+----------------------+-------+----------------------+---------+-------------------+----------------------+-------+----------------------+
            |   id |      price | 100 | deleted |           created-at | phase |                array |    list |               map |                items |  enum |                  xml |
            +------+------------+-----+---------+----------------------+-------+----------------------+---------+-------------------+----------------------+-------+----------------------+
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |       | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | three | <xml><node id="123"> |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |       | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | three | <xml><node id="123"> |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |       | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | three | <xml><node id="123"> |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |       | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | three | <xml><node id="123"> |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |       | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | three | <xml><node id="123"> |
            +------+------------+-----+---------+----------------------+-------+----------------------+---------+-------------------+----------------------+-------+----------------------+
            5 rows

            ASCIITABLE, $etl->display(5));
    }

    public function test_display_partitioned(): void
    {
        $etl = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 0; $i < 5; $i++) {
                        yield rows(
                            schema(
                                int_schema('id'),
                                float_schema('price'),
                                int_schema('100'),
                                bool_schema('deleted'),
                                datetime_schema('created-at'),
                                string_schema('group'),
                            ),
                            // PHP casts the numeric column name "100" to an int array key
                            // @mago-ignore analysis:possibly-invalid-argument
                            row([
                                'id' => 1234,
                                'price' => 123.45,
                                '100' => 100,
                                'deleted' => false,
                                'created-at' => new DateTimeImmutable('2020-07-13 15:00'),
                                'group' => 'A',
                            ]),
                        );
                    }

                    for ($i = 0; $i < 5; $i++) {
                        yield rows(
                            schema(
                                int_schema('id'),
                                float_schema('price'),
                                int_schema('100'),
                                bool_schema('deleted'),
                                datetime_schema('created-at'),
                                string_schema('group'),
                            ),
                            // PHP casts the numeric column name "100" to an int array key
                            // @mago-ignore analysis:possibly-invalid-argument
                            row([
                                'id' => 1234,
                                'price' => 123.45,
                                '100' => 100,
                                'deleted' => false,
                                'created-at' => new DateTimeImmutable('2020-07-13 15:00'),
                                'group' => 'B',
                            ]),
                        );
                    }
                }
            })
            ->collect()
            ->repartition(ref('group'));

        self::assertCommandOutputIdentical(<<<'ASCIITABLE'
            +------+------------+-----+---------+----------------------+-------+
            |   id |      price | 100 | deleted |           created-at | group |
            +------+------------+-----+---------+----------------------+-------+
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     A |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     A |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     A |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     A |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     A |
            +------+------------+-----+---------+----------------------+-------+
            5 rows
            +------+------------+-----+---------+----------------------+-------+
            |   id |      price | 100 | deleted |           created-at | group |
            +------+------------+-----+---------+----------------------+-------+
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     B |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     B |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     B |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     B |
            | 1234 | 123.450000 | 100 |   false | 2020-07-13T15:00:00+ |     B |
            +------+------------+-----+---------+----------------------+-------+
            5 rows

            ASCIITABLE, $etl->display(10));
    }

    public function test_display_with_very_long_entry_name(): void
    {
        $etl = df()
            ->read(from_array([
                [
                    'this is very long entry name that should be longer than items' => [
                        ['id' => 1, 'status' => 'NEW'],
                        ['id' => 2, 'status' => 'PENDING'],
                    ],
                ],
                [
                    'this is very long entry name that should be longer than items' => [
                        ['id' => 1, 'status' => 'NEW'],
                        ['id' => 2, 'status' => 'PENDING'],
                    ],
                ],
                [
                    'this is very long entry name that should be longer than items' => [
                        ['id' => 1, 'status' => 'NEW'],
                        ['id' => 2, 'status' => 'PENDING'],
                    ],
                ],
                [
                    'this is very long entry name that should be longer than items' => [
                        ['id' => 1, 'status' => 'NEW'],
                        ['id' => 2, 'status' => 'PENDING'],
                    ],
                ],
                [
                    'this is very long entry name that should be longer than items' => [
                        ['id' => 1, 'status' => 'NEW'],
                        ['id' => 2, 'status' => 'PENDING'],
                    ],
                ],
                [
                    'this is very long entry name that should be longer than items' => [
                        ['id' => 1, 'status' => 'NEW'],
                        ['id' => 2, 'status' => 'PENDING'],
                    ],
                ],
            ]))
            ->collect();

        self::assertCommandOutputContains(<<<'ASCIITABLE'
            +----------------------+
            | this is very long en |
            +----------------------+
            | [{"id":1,"status":"N |
            | [{"id":1,"status":"N |
            | [{"id":1,"status":"N |
            | [{"id":1,"status":"N |
            | [{"id":1,"status":"N |
            +----------------------+
            5 rows
            ASCIITABLE, $etl->display(5));
    }

    public function test_print_rows(): void
    {
        ob_start();
        df()
            ->read(from_rows(
                rows(
                    schema(int_schema('id'), str_schema('country'), int_schema('age')),
                    row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                    row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                    row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                ),
                rows(
                    schema(
                        int_schema('id'),
                        str_schema('country'),
                        int_schema('age'),
                        int_schema('salary', nullable: true),
                    ),
                    row(['id' => 1, 'country' => 'PL', 'age' => 20, 'salary' => 5000]),
                    row(['id' => 1, 'country' => 'PL', 'age' => 20, 'salary' => null]),
                ),
            ))
            ->printRows();
        $output = ob_get_clean() ?: '';

        // from_rows() folds its batches into one shape and matches each batch to it, so the first
        // batch carries the nullable "salary" column the second one introduced
        self::assertCommandOutputContains(<<<'ASCII'
            +----+---------+-----+--------+
            | id | country | age | salary |
            +----+---------+-----+--------+
            |  1 |      PL |  20 |        |
            |  2 |      PL |  20 |        |
            |  3 |      PL |  25 |        |
            +----+---------+-----+--------+
            3 rows
            +----+---------+-----+--------+
            | id | country | age | salary |
            +----+---------+-----+--------+
            |  1 |      PL |  20 |   5000 |
            |  1 |      PL |  20 |        |
            +----+---------+-----+--------+
            2 rows
            ASCII, $output);
    }

    public function test_print_schema(): void
    {
        ob_start();
        df()
            ->read(from_rows(
                rows(
                    schema(int_schema('id'), str_schema('country'), int_schema('age')),
                    row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                    row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                    row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                ),
                rows(
                    schema(
                        int_schema('id'),
                        str_schema('country'),
                        int_schema('age'),
                        int_schema('salary', nullable: true),
                    ),
                    row(['id' => 1, 'country' => 'PL', 'age' => 20, 'salary' => 5000]),
                    row(['id' => 1, 'country' => 'PL', 'age' => 20, 'salary' => null]),
                ),
            ))
            ->printSchema();
        $output = ob_get_clean() ?: '';

        // from_rows() folds its batches into one shape, so both batches report it
        self::assertCommandOutputContains(<<<'ASCII'
            schema
            |-- id: integer
            |-- country: string
            |-- age: integer
            |-- salary: ?integer
            schema
            |-- id: integer
            |-- country: string
            |-- age: integer
            |-- salary: ?integer
            ASCII, $output);
    }
}
