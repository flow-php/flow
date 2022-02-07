<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Transformer;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

final class ETLTest extends TestCase
{
    public function test_etl_process_constructor() : void
    {
        $collectedRows = ETL::process(
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
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        ETL::extract($extractor)
            ->onError(new IgnoreError())
            ->transform($addStampStringEntry)
            ->transform(new class implements Transformer {
                public function transform(Rows $rows) : Rows
                {
                    throw new \RuntimeException('Unexpected exception');
                }
            })
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'one'))
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'two'))
            ->transform(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'three'))
            ->load($loader)
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

    public function test_first_and_last_rows() : void
    {
        $callback = function (int $index, Rows $rows) : void {
            if ($index === 0) {
                Assert::assertTrue($rows->isFirst());
            }

            if ($index === 3) {
                Assert::assertTrue($rows->isLast());
            }
        };

        ETL::extract(new class implements Extractor {
            /**
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract() : \Generator
            {
                yield new Rows(Row::create(new IntegerEntry('id', 1)));
                yield new Rows(Row::create(new IntegerEntry('id', 2)));
                yield new Rows(Row::create(new IntegerEntry('id', 3)));
            }
        })
            ->onError(new ThrowError())
            ->load(new class($callback) implements Loader {
                private int $index = 0;

                private $callback;

                public function __construct(callable $callback)
                {
                    $this->callback = $callback;
                }

                public function load(Rows $rows) : void
                {
                    ($this->callback)($this->index, $rows);
                    $this->index++;
                }
            })->run();
    }

    public function test_etl_with_collecting() : void
    {
        ETL::extract(
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
                }
            )
            ->collect()
            ->load(
                new class implements Loader {
                    public function load(Rows $rows) : void
                    {
                        Assert::assertCount(3, $rows);
                        Assert::assertTrue($rows->isFirst());
                        Assert::assertTrue($rows->isLast());
                    }
                }
            )
            ->run();
    }

    public function test_etl_with_parallelizing() : void
    {
        ETL::extract(
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
                }
            )
            ->parallelize(2)
            ->load(
                new class implements Loader {
                    public function load(Rows $rows) : void
                    {
                        Assert::assertCount(2, $rows);
                    }
                }
            )
            ->run();
    }

    public function test_etl_fetch_with_limit() : void
    {
        $rows = ETL::extract(
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
        $rows = ETL::extract(
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

    public function test_etl_display() : void
    {
        $etl = ETL::extract(
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
                                new BooleanEntry('deleted', false),
                                new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable('2020-07-13 15:00')),
                                new NullEntry('phase'),
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
                                new Row\Entry\ObjectEntry('object', new \ArrayIterator([1, 2, 3]))
                            ),
                        );
                    }
                }
            }
        );

        $this->assertStringContainsString(
            <<<'ASCIITABLE'
+----+------+-------+--------------------+-----+--------------------+--------------------+--------------------+
|  id| price|deleted|          created-at|phase|               items|                tags|              object|
+----+------+-------+--------------------+-----+--------------------+--------------------+--------------------+
|1234|123.45|  false|2020-07-13T15:00:...| null|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|
|1234|123.45|  false|2020-07-13T15:00:...| null|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|
|1234|123.45|  false|2020-07-13T15:00:...| null|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|
|1234|123.45|  false|2020-07-13T15:00:...| null|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|
|1234|123.45|  false|2020-07-13T15:00:...| null|{"item-id":"1","n...|[{"item-id":"1","...|ArrayIterator Obj...|
+----+------+-------+--------------------+-----+--------------------+--------------------+--------------------+
5 rows
ASCIITABLE,
            $etl->display(5)
        );

        $this->assertStringContainsString(
            <<<'ASCIITABLE'
+----+------+-------+-------------------------+-----+----------------------------+------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+
|  id| price|deleted|               created-at|phase|                       items|                                                                                      tags|                                                                                        object|
+----+------+-------+-------------------------+-----+----------------------------+------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+
|1234|123.45|  false|2020-07-13T15:00:00+00:00| null|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|
|1234|123.45|  false|2020-07-13T15:00:00+00:00| null|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|
|1234|123.45|  false|2020-07-13T15:00:00+00:00| null|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|
|1234|123.45|  false|2020-07-13T15:00:00+00:00| null|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|
|1234|123.45|  false|2020-07-13T15:00:00+00:00| null|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|
|1234|123.45|  false|2020-07-13T15:00:00+00:00| null|{"item-id":"1","name":"one"}|[{"item-id":"1","name":"one"},{"item-id":"2","name":"two"},{"item-id":"3","name":"three"}]|ArrayIterator Object( [storage:ArrayIterator:private] => Array ( [0] => 1 [1] => 2 [2] => 3 ))|
+----+------+-------+-------------------------+-----+----------------------------+------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------+
6 rows
ASCIITABLE,
            $etl->display(6, 0)
        );
    }
}
