<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\xml_entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class DisplayTest extends IntegrationTestCase
{
    public function test_display() : void
    {
        $etl = df()
            ->read(new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    for ($i = 0; $i < 20; $i++) {
                        yield rows(
                            row(
                                int_entry('id', 1234),
                                float_entry('price', 123.45),
                                int_entry('100', 100),
                                bool_entry('deleted', false),
                                datetime_entry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
                                null_entry('phase'),
                                array_entry(
                                    'array',
                                    [
                                        ['id' => 1, 'status' => 'NEW'],
                                        ['id' => 2, 'status' => 'PENDING'],
                                    ]
                                ),
                                list_entry(
                                    'list',
                                    [1, 2, 3],
                                    type_list(type_int())
                                ),
                                map_entry(
                                    'map',
                                    ['NEW', 'PENDING'],
                                    type_map(type_int(), type_string())
                                ),
                                struct_entry(
                                    'items',
                                    ['item-id' => '1', 'name' => 'one'],
                                    struct_type(
                                        struct_element('item-id', type_string()),
                                        struct_element('name', type_string()),
                                    )
                                ),
                                object_entry('object', new \ArrayIterator([1, 2, 3])),
                                enum_entry('enum', BackedStringEnum::three),
                                xml_entry('xml', '<xml><node id="123">test<foo>bar</foo></node></xml>'),
                            ),
                        );
                    }
                }
            })
            ->collect();

        $this->assertSame(
            <<<'ASCIITABLE'
+------+--------+-----+---------+----------------------+-------+----------------------+---------+-------------------+----------------------+----------------------+-------+----------------------+
|   id |  price | 100 | deleted |           created-at | phase |                array |    list |               map |                items |               object |  enum |                  xml |
+------+--------+-----+---------+----------------------+-------+----------------------+---------+-------------------+----------------------+----------------------+-------+----------------------+
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |  null | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | ArrayIterator Object | three | <?xml version="1.0"? |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |  null | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | ArrayIterator Object | three | <?xml version="1.0"? |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |  null | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | ArrayIterator Object | three | <?xml version="1.0"? |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |  null | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | ArrayIterator Object | three | <?xml version="1.0"? |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |  null | [{"id":1,"status":"N | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name | ArrayIterator Object | three | <?xml version="1.0"? |
+------+--------+-----+---------+----------------------+-------+----------------------+---------+-------------------+----------------------+----------------------+-------+----------------------+
5 rows

ASCIITABLE,
            $etl->display(5)
        );
    }

    public function test_display_partitioned() : void
    {
        $etl = df()
            ->read(new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    for ($i = 0; $i < 5; $i++) {
                        yield rows(
                            row(
                                int_entry('id', 1234),
                                float_entry('price', 123.45),
                                int_entry('100', 100),
                                bool_entry('deleted', false),
                                datetime_entry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
                                string_entry('group', 'A')
                            )
                        );
                    }

                    for ($i = 0; $i < 5; $i++) {
                        yield rows(
                            row(
                                int_entry('id', 1234),
                                float_entry('price', 123.45),
                                int_entry('100', 100),
                                bool_entry('deleted', false),
                                datetime_entry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
                                string_entry('group', 'B')
                            )
                        );
                    }
                }
            })
            ->collect()
            ->partitionBy(ref('group'));

        $this->assertSame(
            <<<'ASCIITABLE'
+------+--------+-----+---------+----------------------+-------+
|   id |  price | 100 | deleted |           created-at | group |
+------+--------+-----+---------+----------------------+-------+
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     A |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     A |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     A |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     A |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     A |
+------+--------+-----+---------+----------------------+-------+
Partitions:
 - group=A
5 rows
+------+--------+-----+---------+----------------------+-------+
|   id |  price | 100 | deleted |           created-at | group |
+------+--------+-----+---------+----------------------+-------+
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     B |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     B |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     B |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     B |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+ |     B |
+------+--------+-----+---------+----------------------+-------+
Partitions:
 - group=B
5 rows

ASCIITABLE,
            $etl->display(10)
        );
    }

    public function test_display_with_very_long_entry_name() : void
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

        $this->assertStringContainsString(
            <<<'ASCIITABLE'
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
ASCIITABLE,
            $etl->display(5)
        );
    }

    public function test_print_rows() : void
    {
        \ob_start();
        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                ),
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), int_entry('salary', 5000)),
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), null_entry('salary')),
                )
            ))
            ->printRows();
        $output = \ob_get_clean();

        $this->assertStringContainsString(
            <<<'ASCII'
+----+---------+-----+
| id | country | age |
+----+---------+-----+
|  1 |      PL |  20 |
|  2 |      PL |  20 |
|  3 |      PL |  25 |
+----+---------+-----+
3 rows
+----+---------+-----+--------+
| id | country | age | salary |
+----+---------+-----+--------+
|  1 |      PL |  20 |   5000 |
|  1 |      PL |  20 |   null |
+----+---------+-----+--------+
2 rows
ASCII,
            $output
        );
    }

    public function test_print_schema() : void
    {
        \ob_start();
        df()
            ->read(from_rows(
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                    row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                ),
                rows(
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), int_entry('salary', 5000)),
                    row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), null_entry('salary')),
                )
            ))
            ->printSchema();
        $output = \ob_get_clean();

        $this->assertStringContainsString(
            <<<'ASCII'
schema
|-- id: integer
|-- country: string
|-- age: integer
schema
|-- id: integer
|-- country: string
|-- age: integer
|-- salary: ?integer
ASCII,
            $output
        );
    }
}
