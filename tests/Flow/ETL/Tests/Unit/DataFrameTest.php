<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\Transform;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Flow;
use Flow\ETL\GroupBy\Aggregation;
use Flow\ETL\Join\Condition;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Transformer;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

final class DataFrameTest extends TestCase
{
    public function test_drop() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        )
            ->drop('id')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::string('name', 'bar'), Entry::boolean('active', false)),
            ),
            $rows
        );
    }

    public function test_etl() : void
    {
        $extractor = new class implements Extractor {
            /**
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract() : \Generator
            {
                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 101),
                        new BooleanEntry('deleted', false),
                        new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                        new NullEntry('phase')
                    )
                );

                yield new Rows(
                    Row::create(
                        new IntegerEntry('id', 102),
                        new BooleanEntry('deleted', true),
                        new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                        new NullEntry('phase')
                    )
                );
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public function transform(Rows $rows) : Rows
            {
                return $rows->map(
                    fn (Row $row) : Row => $row->set(new StringEntry('stamp', 'zero'))
                );
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        (new Flow())->read($extractor)
            ->onError(new IgnoreError())
            ->rows($addStampStringEntry)
            ->rows(new class implements Transformer {
                public function transform(Rows $rows) : Rows
                {
                    throw new \RuntimeException('Unexpected exception');
                }

                public function __serialize() : array
                {
                    return [];
                }

                public function __unserialize(array $data) : void
                {
                }
            })
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'one'))
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'two'))
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'three'))
            ->write($loader)
            ->run();

        $this->assertEquals(
            [
                [
                    'id' => 101,
                    'stamp' => 'zero:one:two:three',
                    'deleted' => false,
                    'expiration-date' => new \DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ],
                [
                    'id' => 102,
                    'stamp' => 'zero:one:two:three',
                    'deleted' => true,
                    'expiration-date' => new \DateTimeImmutable('2020-08-25'),
                    'phase' => null,
                ],
            ],
            $loader->result,
        );
    }

    public function test_etl_display() : void
    {
        $etl = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 20; $i++) {
                        yield new Rows(
                            Row::create(
                                new IntegerEntry('id', 1234),
                                new FloatEntry('price', 123.45),
                                new IntegerEntry('100', 100),
                                new BooleanEntry('deleted', false),
                                new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
                                new NullEntry('phase'),
                                new ArrayEntry(
                                    'array',
                                    [
                                        ['id' => 1, 'status' => 'NEW'],
                                        ['id' => 2, 'status' => 'PENDING'],
                                    ]
                                ),
                                new StructureEntry(
                                    'items',
                                    new IntegerEntry('item-id', 1),
                                    new StringEntry('name', 'one'),
                                ),
                                new Row\Entry\CollectionEntry(
                                    'tags',
                                    new Row\Entries(new IntegerEntry('item-id', 1), new StringEntry('name', 'one')),
                                    new Row\Entries(new IntegerEntry('item-id', 2), new StringEntry('name', 'two')),
                                    new Row\Entries(new IntegerEntry('item-id', 3), new StringEntry('name', 'three'))
                                ),
                                new Row\Entry\ObjectEntry('object', new \ArrayIterator([1, 2, 3])),
                                new Row\Entry\EnumEntry('enum', BackedStringEnum::three)
                            ),
                        );
                    }
                }
            }
        );

        $this->assertSame(
            <<<'ASCIITABLE'
+----+------+---+-------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+-----+
|  id| price|100|deleted|          created-at|phase|               array|               items|                tags|              object| enum|
+----+------+---+-------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+-----+
|1234|123.45|100|  false|2020-07-13T15:00:...| null|[{"id":1,"status"...|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|three|
|1234|123.45|100|  false|2020-07-13T15:00:...| null|[{"id":1,"status"...|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|three|
|1234|123.45|100|  false|2020-07-13T15:00:...| null|[{"id":1,"status"...|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|three|
|1234|123.45|100|  false|2020-07-13T15:00:...| null|[{"id":1,"status"...|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|three|
|1234|123.45|100|  false|2020-07-13T15:00:...| null|[{"id":1,"status"...|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|three|
+----+------+---+-------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+-----+
5 rows

ASCIITABLE,
            $etl->display(5)
        );

        $this->assertSame(
            <<<'ASCIITABLE'
+----+------+---+-------+-------------------------+-----+-----------------------------------------------------+----------------------------+------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----+
|  id| price|100|deleted|               created-at|phase|                                                array|                       items|                                                                                      tags|                                                                                        object| enum|
+----+------+---+-------+-------------------------+-----+-----------------------------------------------------+----------------------------+------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----+
|1234|123.45|100|  false|2020-07-13T15:00:00+00:00| null|[{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|three|
|1234|123.45|100|  false|2020-07-13T15:00:00+00:00| null|[{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|three|
|1234|123.45|100|  false|2020-07-13T15:00:00+00:00| null|[{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|three|
|1234|123.45|100|  false|2020-07-13T15:00:00+00:00| null|[{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|three|
|1234|123.45|100|  false|2020-07-13T15:00:00+00:00| null|[{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|three|
|1234|123.45|100|  false|2020-07-13T15:00:00+00:00| null|[{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|three|
+----+------+---+-------+-------------------------+-----+-----------------------------------------------------+----------------------------+------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+-----+
6 rows

ASCIITABLE,
            $etl->display(6, 0)
        );
    }

    public function test_etl_display_with_very_long_entry_name() : void
    {
        $etl = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 20; $i++) {
                        yield new Rows(
                            Row::create(
                                new ArrayEntry(
                                    'this is very long entry name that should be longer than items',
                                    [
                                        ['id' => 1, 'status' => 'NEW'],
                                        ['id' => 2, 'status' => 'PENDING'],
                                    ]
                                ),
                            ),
                        );
                    }
                }
            }
        );

        $this->assertStringContainsString(
            <<<'ASCIITABLE'
+--------------------+
|this is very long...|
+--------------------+
|[{"id":1,"status"...|
|[{"id":1,"status"...|
|[{"id":1,"status"...|
|[{"id":1,"status"...|
|[{"id":1,"status"...|
+--------------------+
5 rows
ASCIITABLE,
            $etl->display(5)
        );

        $this->assertStringContainsString(
            <<<'ASCIITABLE'
+-------------------------------------------------------------+
|this is very long entry name that should be longer than items|
+-------------------------------------------------------------+
|        [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|
|        [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|
|        [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|
|        [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|
|        [{"id":1,"status":"NEW"},{"id":2,"status":"PENDING"}]|
+-------------------------------------------------------------+
5 rows
ASCIITABLE,
            $etl->display(5, 0)
        );
    }

    public function test_etl_exceeding_the_limit_in_one_rows_set() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i + 1)),
                            Row::create(new IntegerEntry('id', $i + 2)),
                        );
                    }
                }
            }
        )
            ->limit(9)
            ->fetch();

        $this->assertCount(9, $rows);
    }

    public function test_etl_fetch_limit_with_closure() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
            ->transform($transformer = new class implements Closure, Transformer {
                public bool $closureCalled = false;

                public int $rowsTransformed = 0;

                public function transform(Rows $rows) : Rows
                {
                    $this->rowsTransformed += 1;

                    return $rows;
                }

                public function closure(Rows $rows) : void
                {
                    $this->closureCalled = true;
                }

                public function __serialize() : array
                {
                    return [];
                }

                public function __unserialize(array $data) : void
                {
                }
            })
            ->fetch(10);

        $this->assertCount(10, $rows);
        $this->assertSame(10, $transformer->rowsTransformed);
        $this->assertTrue($transformer->closureCalled);
    }

    public function test_etl_fetch_with_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
        ->fetch(10);

        $this->assertCount(10, $rows);
    }

    public function test_etl_fetch_without_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 20; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
        ->fetch();

        $this->assertCount(20, $rows);
    }

    public function test_etl_filter() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
            ->filter(fn (Row $row) => $row->valueOf('id') % 2 === 0)
            ->fetch();

        $this->assertCount(5, $rows);
        $this->assertSame(
            [['id' => 2], ['id' => 4], ['id' => 6], ['id' => 8], ['id' => 10]],
            $rows->toArray()
        );
    }

    public function test_etl_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i + 1)),
                            Row::create(new IntegerEntry('id', $i + 2)),
                        );
                    }
                }
            }
        )
        ->limit(10)
        ->fetch();

        $this->assertCount(10, $rows);
    }

    public function test_etl_limit_with_closure() : void
    {
        (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
            ->transform($transformer = new class implements Closure, Transformer {
                public bool $closureCalled = false;

                public int $rowsTransformed = 0;

                public function transform(Rows $rows) : Rows
                {
                    $this->rowsTransformed += 1;

                    return $rows;
                }

                public function closure(Rows $rows) : void
                {
                    $this->closureCalled = true;
                }

                public function __serialize() : array
                {
                    return [];
                }

                public function __unserialize(array $data) : void
                {
                }
            })
            ->limit(10)
            ->run();

        $this->assertSame(10, $transformer->rowsTransformed);
        $this->assertTrue($transformer->closureCalled);
    }

    public function test_etl_limit_with_collecting() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i + 1)),
                            Row::create(new IntegerEntry('id', $i + 2)),
                        );
                    }
                }
            }
        )
            ->limit(10)
            ->collect()
            ->fetch();

        $this->assertCount(10, $rows);
    }

    public function test_etl_limit_with_parallelizing() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i + 1)),
                            Row::create(new IntegerEntry('id', $i + 2)),
                        );
                    }
                }
            }
        )
            ->parallelize(50)
            ->limit(10)
            ->fetch();

        $this->assertCount(10, $rows);
    }

    public function test_etl_map() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
            ->map(fn (Row $row) => $row->add(new BooleanEntry('odd', $row->valueOf('id') % 2 === 0)))
            ->fetch();

        $this->assertCount(10, $rows);
        $this->assertSame(
            [
                ['id' => 1, 'odd' => false],
                ['id' => 2, 'odd' => true],
                ['id' => 3, 'odd' => false],
                ['id' => 4, 'odd' => true],
                ['id' => 5, 'odd' => false],
                ['id' => 6, 'odd' => true],
                ['id' => 7, 'odd' => false],
                ['id' => 8, 'odd' => true],
                ['id' => 9, 'odd' => false],
                ['id' => 10, 'odd' => true],
            ],
            $rows->toArray()
        );
    }

    public function test_etl_process_constructor() : void
    {
        $collectedRows = (new Flow())->process(
            $rows = new Rows(
                Row::create(
                    new IntegerEntry('id', 101),
                    new BooleanEntry('deleted', false),
                    new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                    new NullEntry('phase')
                )
            )
        )
            ->fetch();

        $this->assertEquals($rows, $collectedRows);
    }

    public function test_etl_with_collecting() : void
    {
        (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    yield new Rows(Row::create(new IntegerEntry('id', 1)));
                    yield new Rows(Row::create(new IntegerEntry('id', 2)));
                    yield new Rows(Row::create(new IntegerEntry('id', 3)));
                }
            }
        )
            ->transform(
                new class implements Transformer {
                    public function transform(Rows $rows) : Rows
                    {
                        return $rows->map(fn (Row $row) => $row->rename('id', 'new_id'));
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                }
            )
            ->collect()
            ->load(
                new class implements Loader {
                    public function load(Rows $rows) : void
                    {
                        Assert::assertCount(3, $rows);
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                }
            )
            ->run();
    }

    public function test_etl_with_parallelizing() : void
    {
        (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    yield new Rows(
                        Row::create(new IntegerEntry('id', 1)),
                        Row::create(new IntegerEntry('id', 2)),
                        Row::create(new IntegerEntry('id', 3)),
                        Row::create(new IntegerEntry('id', 4)),
                        Row::create(new IntegerEntry('id', 5)),
                        Row::create(new IntegerEntry('id', 6)),
                        Row::create(new IntegerEntry('id', 7)),
                        Row::create(new IntegerEntry('id', 8)),
                        Row::create(new IntegerEntry('id', 9)),
                        Row::create(new IntegerEntry('id', 10)),
                    );
                }
            }
        )
            ->transform(
                new class implements Transformer {
                    public function transform(Rows $rows) : Rows
                    {
                        return $rows->map(fn (Row $row) => $row->rename('id', 'new_id'));
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                }
            )
            ->parallelize(2)
            ->load(
                new class implements Loader {
                    public function load(Rows $rows) : void
                    {
                        Assert::assertCount(2, $rows);
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                }
            )
            ->run();
    }

    public function test_etl_with_total_rows_below_the_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract() : \Generator
                {
                    for ($i = 0; $i < 5; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
            ->limit(10)
            ->fetch();

        $this->assertCount(5, $rows);
    }

    public function test_fetch_with_limit_below_0() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Fetch limit can't be lower or equal to 0");

        (new Flow())->process(new Rows())
            ->fetch(-1);
    }

    public function test_foreach() : void
    {
        (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        )
            ->foreach(function (Rows $rows) : void {
                $this->assertEquals(
                    new Rows(
                        Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                        Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                        Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
                    ),
                    $rows
                );
            });
    }

    public function test_group_by_multiple_columns_and_parallelize() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(4))
            ->method('load');

        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50), Entry::string('gender', 'male')),
            )
        )
            ->groupBy('country', 'gender')
            ->parallelize(1)
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'male')),
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'female')),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'female')),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'male')),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50), Entry::string('gender', 'male')),
            )
        )
            ->groupBy('country', 'gender')
            ->aggregate(Aggregation::avg('age'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'male'), Entry::float('age_avg', 21.666666666666668)),
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'female'), Entry::integer('age_avg', 30)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'female'), Entry::float('age_avg', 42.5)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'male'), Entry::integer('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_multiples_columns_with_avg_aggregation_with_null() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'female')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40), Entry::string('gender', 'male')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45), Entry::null('gender')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50), Entry::string('gender', 'male')),
            )
        )
            ->groupBy('country', 'gender')
            ->aggregate(Aggregation::avg('age'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'male'), Entry::float('age_avg', 21.666666666666668)),
                Row::create(Entry::string('country', 'PL'), Entry::string('gender', 'female'), Entry::integer('age_avg', 30)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'female'), Entry::integer('age_avg', 40)),
                Row::create(Entry::string('country', 'US'), Entry::string('gender', 'male'), Entry::integer('age_avg', 45)),
                Row::create(Entry::string('country', 'US'), Entry::null('gender'), Entry::integer('age_avg', 45)),
            ),
            $rows
        );
    }

    public function test_group_by_single_column() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->groupBy('country')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL')),
                Row::create(Entry::string('country', 'US')),
            ),
            $rows
        );
    }

    public function test_group_by_single_column_with_avg_aggregation() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->groupBy('country')
            ->aggregate(Aggregation::avg('age'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL'), Entry::float('age_avg', 23.75)),
                Row::create(Entry::string('country', 'US'), Entry::float('age_avg', 43.75)),
            ),
            $rows
        );
    }

    public function test_join() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US')),
            )
        )
            ->parallelize(4)
            ->join(
                (new Flow())->process(
                    new Rows(
                        Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                        Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                    )
                ),
                Condition::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::string('name', 'United States')),
            ),
            $rows
        );
    }

    public function test_join_each() : void
    {
        $loader = $this->createMock(Loader::class);
        $loader->expects($this->exactly(2))
            ->method('load');

        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US')),
            )
        )
            ->parallelize(4)
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows) : DataFrame
                    {
                        return (new Flow())->process(
                            new Rows(
                                Row::create(Entry::string('code', 'PL'), Entry::string('name', 'Poland')),
                                Row::create(Entry::string('code', 'US'), Entry::string('name', 'United States')),
                            )
                        );
                    }

                    public function __serialize() : array
                    {
                        return [];
                    }

                    public function __unserialize(array $data) : void
                    {
                    }
                },
                Condition::on(['country' => 'code']),
            )
            ->write($loader)
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::string('name', 'Poland')),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::string('name', 'United States')),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::string('name', 'United States')),
            ),
            $rows
        );
    }

    public function test_limit_below_0() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        (new Flow())->process(new Rows())
            ->limit(-1);
    }

    public function test_overriding_group_by() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->groupBy('id')
            ->groupBy('country')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'PL')),
                Row::create(Entry::string('country', 'US')),
            ),
            $rows
        );
    }

    public function test_rename() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        )
            ->rename('name', 'new_name')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('new_name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('new_name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('new_name', 'bar'), Entry::boolean('active', false)),
            ),
            $rows
        );
    }

    public function test_select() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        )
            ->select('id')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1)),
                Row::create(Entry::integer('id', 2)),
                Row::create(Entry::integer('id', 2)),
            ),
            $rows
        );
    }

    public function test_selective_validation_against_schema() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::array('tags', ['foo', 'bar'])),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        )->validate(
            new Schema(Schema\Definition::integer('id', $nullable = false)),
            new Schema\SelectiveValidator()
        )->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::array('tags', ['foo', 'bar'])),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            ),
            $rows
        );
    }

    public function test_standalone_avg_aggregation() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->aggregate(Aggregation::avg('age'))
            ->rows(Transform::rename('age_avg', 'average_age'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::float('average_age', 33.75)),
            ),
            $rows
        );
    }

    public function test_standalone_avg_and_max_aggregation() : void
    {
        (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->aggregate(Aggregation::avg('age'), Aggregation::max('age'))
            ->run(function (Rows $rows) : void {
                $this->assertEquals(
                    new Rows(
                        Row::create(
                            Entry::float('age_avg', 33.75),
                            Entry::integer('age_max', 50)
                        )
                    ),
                    $rows
                );
            });
    }

    public function test_strict_validation_against_schema() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            )
        )->validate(
            new Schema(
                Schema\Definition::integer('id', $nullable = false),
                Schema\Definition::string('name', $nullable = true),
                Schema\Definition::boolean('active', $nullable = false),
            )
        )->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('name', 'foo'), Entry::boolean('active', true)),
                Row::create(Entry::integer('id', 2), Entry::null('name'), Entry::boolean('active', false)),
                Row::create(Entry::integer('id', 2), Entry::string('name', 'bar'), Entry::boolean('active', false)),
            ),
            $rows
        );
    }

    public function test_void() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            )
        )
            ->rename('country', 'country_code')
            ->void()
            ->aggregate(Aggregation::avg('age'))
            ->rows(Transform::rename('age_avg', 'average_age'))
            ->fetch();

        $this->assertEquals(
            new Rows(),
            $rows
        );
    }
}
