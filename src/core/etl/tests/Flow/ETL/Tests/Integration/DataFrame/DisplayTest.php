<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\read;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class DisplayTest extends IntegrationTestCase
{
    public function test_display() : void
    {
        $etl = read(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    for ($i = 0; $i < 20; $i++) {
                        yield new Rows(
                            Row::create(
                                new IntegerEntry('id', 1234),
                                new FloatEntry('price', 123.45),
                                new IntegerEntry('100', 100),
                                new BooleanEntry('deleted', false),
                                new DateTimeEntry('created-at', new \DateTimeImmutable('2020-07-13 15:00')),
                                new NullEntry('phase'),
                                new ArrayEntry(
                                    'array',
                                    [
                                        ['id' => 1, 'status' => 'NEW'],
                                        ['id' => 2, 'status' => 'PENDING'],
                                    ]
                                ),
                                new ListEntry(
                                    'list',
                                    [1, 2, 3],
                                    new ListType(ListElement::integer())
                                ),
                                new MapEntry(
                                    'map',
                                    ['NEW', 'PENDING'],
                                    new MapType(MapKey::integer(), MapValue::string())
                                ),
                                new StructureEntry(
                                    'items',
                                    ['item-id' => '1', 'name' => 'one'],
                                    new StructureType(
                                        new StructureElement('item-id', ScalarType::string()),
                                        new StructureElement('name', ScalarType::string()),
                                    )
                                ),
                                new Row\Entry\ObjectEntry('object', new \ArrayIterator([1, 2, 3])),
                                new Row\Entry\EnumEntry('enum', BackedStringEnum::three),
                                new Row\Entry\XMLEntry('xml', '<xml><node id="123">test<foo>bar</foo></node></xml>'),
                            ),
                        );
                    }
                }
            }
        );

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

        $this->assertSame(
            <<<'ASCIITABLE'
+------+--------+-----+---------+---------------------------+-------+-------------------------------------------------------+---------+-------------------+------------------------------+------------------------------------------------------------------------------------------------+-------+--------------------------------------------------------------------------+
|   id |  price | 100 | deleted |                created-at | phase |                                                 array |    list |               map |                        items |                                                                                         object |  enum |                                                                      xml |
+------+--------+-----+---------+---------------------------+-------+-------------------------------------------------------+---------+-------------------+------------------------------+------------------------------------------------------------------------------------------------+-------+--------------------------------------------------------------------------+
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+00:00 |  null | [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name":"one"} | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) | three | <?xml version="1.0"?><xml><node id="123">test<foo>bar</foo></node></xml> |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+00:00 |  null | [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name":"one"} | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) | three | <?xml version="1.0"?><xml><node id="123">test<foo>bar</foo></node></xml> |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+00:00 |  null | [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name":"one"} | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) | three | <?xml version="1.0"?><xml><node id="123">test<foo>bar</foo></node></xml> |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+00:00 |  null | [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name":"one"} | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) | three | <?xml version="1.0"?><xml><node id="123">test<foo>bar</foo></node></xml> |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+00:00 |  null | [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name":"one"} | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) | three | <?xml version="1.0"?><xml><node id="123">test<foo>bar</foo></node></xml> |
| 1234 | 123.45 | 100 |   false | 2020-07-13T15:00:00+00:00 |  null | [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] | [1,2,3] | ["NEW","PENDING"] | {"item-id":"1","name":"one"} | ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 )) | three | <?xml version="1.0"?><xml><node id="123">test<foo>bar</foo></node></xml> |
+------+--------+-----+---------+---------------------------+-------+-------------------------------------------------------+---------+-------------------+------------------------------+------------------------------------------------------------------------------------------------+-------+--------------------------------------------------------------------------+
6 rows

ASCIITABLE,
            $etl->display(6, 0)
        );
    }

    public function test_display_with_very_long_entry_name() : void
    {
        $etl = read(
            from_array([
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
                ])
        );

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

        $this->assertStringContainsString(
            <<<'ASCIITABLE'
+---------------------------------------------------------------+
| this is very long entry name that should be longer than items |
+---------------------------------------------------------------+
|         [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] |
|         [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] |
|         [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] |
|         [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] |
|         [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}] |
+---------------------------------------------------------------+
5 rows
ASCIITABLE,
            $etl->display(5, 0)
        );
    }

    public function test_print_rows() : void
    {
        \ob_start();
        read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
            ),
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::integer('salary', 5000)),
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::null('salary')),
            )
        ))->printRows();
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
        read(from_rows(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
            ),
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::integer('salary', 5000)),
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::null('salary')),
            )
        ))->printSchema();
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
