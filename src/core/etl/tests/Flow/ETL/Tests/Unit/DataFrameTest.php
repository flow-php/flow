<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use Flow\ETL\DataFrame;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\Partitions;
use Flow\ETL\DSL\To;
use Flow\ETL\DSL\Transform;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

final class DataFrameTest extends TestCase
{
    public function test_batch_size() : void
    {
        (new Flow())
            ->read(From::array([
                ['id' => '01', 'elements' => [['sub_id' => '01_01'], ['sub_id' => '01_02']]],
                ['id' => '02', 'elements' => [['sub_id' => '02_01'], ['sub_id' => '02_02']]],
                ['id' => '03', 'elements' => [['sub_id' => '03_01'], ['sub_id' => '03_02']]],
                ['id' => '04', 'elements' => [['sub_id' => '04_01'], ['sub_id' => '04_02']]],
                ['id' => '05', 'elements' => [['sub_id' => '05_01'], ['sub_id' => '05_02'], ['sub_id' => '05_03']]],
            ]))
            ->batchSize(1)
            ->load(To::callback(function (Rows $rows) : void {
                $this->assertCount(1, $rows);
            }))
            ->withEntry('element', ref('elements')->expand())
            ->batchSize(3)
            ->run(function (Rows $rows) : void {
                $this->assertLessThanOrEqual(3, $rows->count());
            });
    }

    public function test_collect_references() : void
    {
        $dataset1 = [
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
        ];
        $dataset2 = [
            ['id' => 1, 'name' => 'test', 'active' => false, 'country' => 'US'],
            ['id' => 1, 'name' => 'test', 'active' => false, 'group' => 'A'],
        ];

        (new Flow())
            ->read(From::chain(
                From::array($dataset1),
                From::array($dataset2),
            ))
            ->collectRefs($refs = refs())
            ->run();

        $this->assertEquals(
            refs('id', 'name', 'active', 'country', 'group'),
            $refs
        );
    }

    public function test_count() : void
    {
        $count = (new Flow())
            ->read(From::array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]))
            ->count();

        $this->assertSame(5, $count);
    }

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

    public function test_drop_duplicates() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(Entry::int('id', 1), Entry::str('name', 'foo'), Entry::bool('active', true)),
                Row::create(Entry::int('id', 2), Entry::str('name', 'bar'), Entry::bool('active', false)),
                Row::create(Entry::int('id', 2), Entry::str('name', 'bar'), Entry::bool('active', false)),
            )
        )
            ->dropDuplicates(ref('id'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::int('id', 1), Entry::str('name', 'foo'), Entry::bool('active', true)),
                Row::create(Entry::int('id', 2), Entry::str('name', 'bar'), Entry::bool('active', false)),
            ),
            $rows
        );
    }

    public function test_encapsulate_transformations() : void
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
            ->rows(new class implements Transformation {
                public function transform(DataFrame $dataFrame) : DataFrame
                {
                    return $dataFrame->withEntry('country', ref('country')->lower())
                        ->withEntry('age', ref('age')->divide(lit(10)));
                }
            })
            ->rows(
                new class implements Transformation {
                    public function transform(DataFrame $dataFrame) : DataFrame
                    {
                        return $dataFrame->drop('gender')
                            ->drop('id');
                    }
                }
            )
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('country', 'pl'), Entry::integer('age', 2)),
                Row::create(Entry::string('country', 'pl'), Entry::integer('age', 2)),
                Row::create(Entry::string('country', 'pl'), Entry::float('age', 2.5)),
                Row::create(Entry::string('country', 'pl'), Entry::integer('age', 3)),
                Row::create(Entry::string('country', 'us'), Entry::integer('age', 4)),
                Row::create(Entry::string('country', 'us'), Entry::integer('age', 4)),
                Row::create(Entry::string('country', 'us'), Entry::float('age', 4.5)),
                Row::create(Entry::string('country', 'us'), Entry::integer('age', 5)),
            ),
            $rows
        );
    }

    public function test_exceeding_the_limit_in_one_rows_set() : void
    {
        $rows = (new Flow())
            ->read(
                From::array(\array_map(
                    fn (int $id) : array => ['id' => $id],
                    \range(1, 1000)
                ))
            )
            ->limit(9)
            ->fetch();

        $this->assertCount(9, $rows);
    }

    public function test_fetch_with_limit() : void
    {
        $rows = (new Flow())
            ->read(From::array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ]))
            ->fetch(5);

        $this->assertCount(5, $rows);
    }

    public function test_fetch_with_limit_below_0() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        (new Flow())->process(new Rows())
            ->fetch(-1);
    }

    public function test_fetch_without_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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

    public function test_filter() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield new Rows(
                            Row::create(new IntegerEntry('id', $i)),
                        );
                    }
                }
            }
        )
            ->filter(ref('id')->mod(lit(2))->same(lit(0)))
            ->fetch();

        $this->assertCount(5, $rows);
        $this->assertSame(
            [['id' => 2], ['id' => 4], ['id' => 6], ['id' => 8], ['id' => 10]],
            $rows->toArray()
        );
    }

    public function test_filter_partitions() : void
    {
        $partitionedRows = (new Flow())->process(
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
            ->partitionBy('country')
            ->filterPartitions(Partitions::chain(Partitions::only('country', 'US')))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40)),
                Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
            ),
            $partitionedRows
        );
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

    public function test_get() : void
    {
        $rows = (new Flow())
            ->extract(From::rows(
                $extractedRows = new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 2), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 3), Entry::string('name', 'baz')),
                    Row::create(Entry::integer('id', 4), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 5), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 6), Entry::string('name', 'baz')),
                )
            ))
            ->get();

        $this->assertEquals([$extractedRows], \iterator_to_array($rows));
    }

    public function test_get_as_array() : void
    {
        $rows = (new Flow())
            ->extract(From::rows(
                $extractedRows = new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 2), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 3), Entry::string('name', 'baz')),
                    Row::create(Entry::integer('id', 4), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 5), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 6), Entry::string('name', 'baz')),
                )
            ))
            ->getAsArray();

        $this->assertEquals([
            $extractedRows->toArray(),
        ], \iterator_to_array($rows));
    }

    public function test_get_each() : void
    {
        $rows = (new Flow())
            ->extract(From::rows(
                $extractedRows = new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 2), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 3), Entry::string('name', 'baz')),
                    Row::create(Entry::integer('id', 4), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 5), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 6), Entry::string('name', 'baz')),
                )
            ))
            ->getEach();

        $this->assertEquals([
            Row::create(Entry::integer('id', 1), Entry::string('name', 'foo')),
            Row::create(Entry::integer('id', 2), Entry::string('name', 'bar')),
            Row::create(Entry::integer('id', 3), Entry::string('name', 'baz')),
            Row::create(Entry::integer('id', 4), Entry::string('name', 'foo')),
            Row::create(Entry::integer('id', 5), Entry::string('name', 'bar')),
            Row::create(Entry::integer('id', 6), Entry::string('name', 'baz')),
        ], \iterator_to_array($rows));
    }

    public function test_get_each_as_array() : void
    {
        $rows = (new Flow())
            ->extract(From::rows(
                $extractedRows = new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 2), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 3), Entry::string('name', 'baz')),
                    Row::create(Entry::integer('id', 4), Entry::string('name', 'foo')),
                    Row::create(Entry::integer('id', 5), Entry::string('name', 'bar')),
                    Row::create(Entry::integer('id', 6), Entry::string('name', 'baz')),
                )
            ))
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'foo'],
                ['id' => 2, 'name' => 'bar'],
                ['id' => 3, 'name' => 'baz'],
                ['id' => 4, 'name' => 'foo'],
                ['id' => 5, 'name' => 'bar'],
                ['id' => 6, 'name' => 'baz'],
            ],
            \iterator_to_array($rows)
        );
    }

    public function test_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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

    public function test_limit_below_0() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        (new Flow())->process(new Rows())
            ->limit(-1);
    }

    public function test_limit_when_transformation_is_expanding_rows_extracted_from_extractor() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield new Rows(
                            Row::create(new ArrayEntry('ids', [
                                ['id' => $i + 1, 'more_ids' => [['more_id' => $i + 4], ['more_id' => $i + 7]]],
                                ['id' => $i + 2, 'more_ids' => [['more_id' => $i + 5], ['more_id' => $i + 8]]],
                                ['id' => $i + 3, 'more_ids' => [['more_id' => $i + 6], ['more_id' => $i + 9]]],
                            ])),
                        );
                    }
                }
            }
        )
            ->withEntries([
                'expanded' => ref('ids')->expand(),
                'element' => ref('expanded')->unpack(),
                'more_ids' => ref('element.more_ids')->expand(),
            ])
            ->rename('element.id', 'id')
            ->drop('expanded', 'ids', 'element', 'element.more_ids')
            ->limit(3)
            ->fetch();

        $this->assertCount(3, $rows);
    }

    public function test_limit_with_batch_size() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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
            ->batchSize(50)
            ->limit(10)
            ->fetch();

        $this->assertCount(10, $rows);
    }

    public function test_limit_with_collecting() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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

    public function test_map() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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

    public function test_partition_by() : void
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
            ->partitionBy(ref('country'))
            ->batchSize(2) // split each partition into two
            ->get();

        $this->assertEquals(
            [
                new Rows(
                    Row::create(Entry::integer('id', 1), Entry::string('country', 'PL'), Entry::integer('age', 20)),
                    Row::create(Entry::integer('id', 2), Entry::string('country', 'PL'), Entry::integer('age', 20))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 3), Entry::string('country', 'PL'), Entry::integer('age', 25)),
                    Row::create(Entry::integer('id', 4), Entry::string('country', 'PL'), Entry::integer('age', 30)),
                ),
                new Rows(
                    Row::create(Entry::integer('id', 5), Entry::string('country', 'US'), Entry::integer('age', 40)),
                    Row::create(Entry::integer('id', 6), Entry::string('country', 'US'), Entry::integer('age', 40))
                ),
                new Rows(
                    Row::create(Entry::integer('id', 7), Entry::string('country', 'US'), Entry::integer('age', 45)),
                    Row::create(Entry::integer('id', 9), Entry::string('country', 'US'), Entry::integer('age', 50)),
                ),
            ],
            \iterator_to_array($rows)
        );
    }

    public function test_pipeline() : void
    {
        $extractor = new class implements Extractor {
            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context) : \Generator
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
            public function transform(Rows $rows, FlowContext $context) : Rows
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

            public function load(Rows $rows, FlowContext $context) : void
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
                public function transform(Rows $rows, FlowContext $context) : Rows
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

    public function test_process_constructor() : void
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

    public function test_rename_all() : void
    {
        $rows = new Rows(
            Row::create(Entry::array('array', ['id' => 1, 'name' => 'name', 'active' => true])),
            Row::create(Entry::array('array', ['id' => 2, 'name' => 'name', 'active' => false]))
        );

        $ds = (new Flow())
            ->read(From::rows($rows))
            ->withEntry('row', ref('array')->unpack())
            ->renameAll('row.', '')
            ->drop('array')
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_lower_case() : void
    {
        $rows = new Rows(
            Row::create(Entry::int('ID', 1), Entry::str('NAME', 'name'), Entry::bool('ACTIVE', true)),
            Row::create(Entry::int('ID', 2), Entry::str('NAME', 'name'), Entry::bool('ACTIVE', false)),
        );

        $ds = (new Flow())
            ->read(From::rows($rows))
            ->renameAllLowerCase()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'name' => 'name', 'active' => true],
                ['id' => 2, 'name' => 'name', 'active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_to_snake_case() : void
    {
        $rows = new Rows(
            Row::create(Entry::int('id', 1), Entry::str('UserName', 'name'), Entry::bool('isActive', true)),
            Row::create(Entry::int('id', 2), Entry::str('UserName', 'name'), Entry::bool('isActive', false)),
        );

        $ds = (new Flow())
            ->read(From::rows($rows))
            ->renameAllStyle(StringStyles::SNAKE)
            ->renameAllLowerCase()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['id' => 1, 'user_name' => 'name', 'is_active' => true],
                ['id' => 2, 'user_name' => 'name', 'is_active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case() : void
    {
        $rows = new Rows(
            Row::create(Entry::int('id', 1), Entry::str('name', 'name'), Entry::bool('active', true)),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name'), Entry::bool('active', false)),
        );

        $ds = (new Flow())
            ->read(From::rows($rows))
            ->renameAllUpperCase()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['ID' => 1, 'NAME' => 'name', 'ACTIVE' => true],
                ['ID' => 2, 'NAME' => 'name', 'ACTIVE' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_first() : void
    {
        $rows = new Rows(
            Row::create(Entry::int('id', 1), Entry::str('name', 'name'), Entry::bool('active', true)),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name'), Entry::bool('active', false)),
        );

        $ds = (new Flow())
            ->read(From::rows($rows))
            ->renameAllUpperCaseFirst()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds)
        );
    }

    public function test_rename_all_upper_case_word() : void
    {
        $rows = new Rows(
            Row::create(Entry::int('id', 1), Entry::str('name', 'name'), Entry::bool('active', true)),
            Row::create(Entry::int('id', 2), Entry::str('name', 'name'), Entry::bool('active', false)),
        );

        $ds = (new Flow())
            ->read(From::rows($rows))
            ->renameAllUpperCaseWord()
            ->getEachAsArray();

        $this->assertEquals(
            [
                ['Id' => 1, 'Name' => 'name', 'Active' => true],
                ['Id' => 2, 'Name' => 'name', 'Active' => false],
            ],
            \iterator_to_array($ds)
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
            ->select('name', 'id')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('name', 'foo'), Entry::integer('id', 1)),
                Row::create(Entry::null('name'), Entry::integer('id', 2)),
                Row::create(Entry::string('name', 'bar'), Entry::integer('id', 2)),
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

    public function test_until() : void
    {
        $rows = (new Flow())
            ->read(From::chain(
                From::array([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                    ['id' => 4],
                    ['id' => 5],
                ]),
                From::array([
                    ['id' => 6],
                    ['id' => 7],
                    ['id' => 8],
                    ['id' => 9],
                    ['id' => 10],
                ])
            ))
            ->until(ref('id')->lessThanEqual(lit(3)))
            ->fetch();

        $this->assertCount(3, $rows);
        $this->assertSame(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $rows->toArray()
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
            ->aggregate(average(ref('age')))
            ->rows(Transform::rename('age_avg', 'average_age'))
            ->fetch();

        $this->assertEquals(
            new Rows(),
            $rows
        );
    }

    public function test_with_batch_size() : void
    {
        (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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
                    public function transform(Rows $rows, FlowContext $context) : Rows
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
            ->batchSize(2)
            ->load(
                new class implements Loader {
                    public function load(Rows $rows, FlowContext $context) : void
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

    public function test_with_collecting() : void
    {
        (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    yield new Rows(Row::create(new IntegerEntry('id', 1)));
                    yield new Rows(Row::create(new IntegerEntry('id', 2)));
                    yield new Rows(Row::create(new IntegerEntry('id', 3)));
                }
            }
        )
            ->transform(
                new class implements Transformer {
                    public function transform(Rows $rows, FlowContext $context) : Rows
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
                    public function load(Rows $rows, FlowContext $context) : void
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

    public function test_with_total_rows_below_the_limit() : void
    {
        $rows = (new Flow())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
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
}
