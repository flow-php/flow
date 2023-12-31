<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_callable;
use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Extractor;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
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
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

final class DataFrameTest extends TestCase
{
    public function test_batch_size() : void
    {
        df()
            ->read(from_array([
                ['id' => '01', 'elements' => [['sub_id' => '01_01'], ['sub_id' => '01_02']]],
                ['id' => '02', 'elements' => [['sub_id' => '02_01'], ['sub_id' => '02_02']]],
                ['id' => '03', 'elements' => [['sub_id' => '03_01'], ['sub_id' => '03_02']]],
                ['id' => '04', 'elements' => [['sub_id' => '04_01'], ['sub_id' => '04_02']]],
                ['id' => '05', 'elements' => [['sub_id' => '05_01'], ['sub_id' => '05_02'], ['sub_id' => '05_03']]],
            ]))
            ->batchSize(1)
            ->load(to_callable(function (Rows $rows) : void {
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

        df()
            ->read(from_all(
                from_array($dataset1),
                from_array($dataset2),
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
        $count = df()
            ->read(from_array([
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
        $rows = df()->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )
        )
            ->drop('id')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(null_entry('name'), bool_entry('active', false)),
                Row::create(str_entry('name', 'bar'), bool_entry('active', false)),
            ),
            $rows
        );
    }

    public function test_drop_duplicates() : void
    {
        $rows = df()->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )
        )
            ->dropDuplicates(ref('id'))
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ),
            $rows
        );
    }

    public function test_encapsulate_transformations() : void
    {
        $rows = df()->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')),
                Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')),
                Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', 'female')),
                Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
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
                Row::create(str_entry('country', 'pl'), int_entry('age', 2)),
                Row::create(str_entry('country', 'pl'), int_entry('age', 2)),
                Row::create(str_entry('country', 'pl'), float_entry('age', 2.5)),
                Row::create(str_entry('country', 'pl'), int_entry('age', 3)),
                Row::create(str_entry('country', 'us'), int_entry('age', 4)),
                Row::create(str_entry('country', 'us'), int_entry('age', 4)),
                Row::create(str_entry('country', 'us'), float_entry('age', 4.5)),
                Row::create(str_entry('country', 'us'), int_entry('age', 5)),
            ),
            $rows
        );
    }

    public function test_filter() : void
    {
        $rows = df()->extract(
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

    public function test_foreach() : void
    {
        df()->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )
        )
            ->foreach(function (Rows $rows) : void {
                $this->assertEquals(
                    new Rows(
                        Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                        Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                        Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
                    ),
                    $rows
                );
            });
    }

    public function test_get() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 2), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 3), str_entry('name', 'baz')),
                    Row::create(int_entry('id', 4), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 5), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 6), str_entry('name', 'baz')),
                )
            ))
            ->get();

        $this->assertEquals([$extractedRows], \iterator_to_array($rows));
    }

    public function test_get_as_array() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 2), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 3), str_entry('name', 'baz')),
                    Row::create(int_entry('id', 4), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 5), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 6), str_entry('name', 'baz')),
                )
            ))
            ->getAsArray();

        $this->assertEquals([
            $extractedRows->toArray(),
        ], \iterator_to_array($rows));
    }

    public function test_get_each() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 2), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 3), str_entry('name', 'baz')),
                    Row::create(int_entry('id', 4), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 5), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 6), str_entry('name', 'baz')),
                )
            ))
            ->getEach();

        $this->assertEquals([
            Row::create(int_entry('id', 1), str_entry('name', 'foo')),
            Row::create(int_entry('id', 2), str_entry('name', 'bar')),
            Row::create(int_entry('id', 3), str_entry('name', 'baz')),
            Row::create(int_entry('id', 4), str_entry('name', 'foo')),
            Row::create(int_entry('id', 5), str_entry('name', 'bar')),
            Row::create(int_entry('id', 6), str_entry('name', 'baz')),
        ], \iterator_to_array($rows));
    }

    public function test_get_each_as_array() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = new Rows(
                    Row::create(int_entry('id', 1), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 2), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 3), str_entry('name', 'baz')),
                    Row::create(int_entry('id', 4), str_entry('name', 'foo')),
                    Row::create(int_entry('id', 5), str_entry('name', 'bar')),
                    Row::create(int_entry('id', 6), str_entry('name', 'baz')),
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

    public function test_select() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )
        )
            ->select('name', 'id')
            ->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('name', 'foo'), int_entry('id', 1)),
                Row::create(null_entry('name'), int_entry('id', 2)),
                Row::create(str_entry('name', 'bar'), int_entry('id', 2)),
            ),
            $rows
        );
    }

    public function test_selective_validation_against_schema() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), array_entry('tags', ['foo', 'bar'])),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )
        )->validate(
            new Schema(Schema\Definition::integer('id', $nullable = false)),
            new Schema\SelectiveValidator()
        )->fetch();

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), array_entry('tags', ['foo', 'bar'])),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ),
            $rows
        );
    }

    public function test_strict_validation_against_schema() : void
    {
        $rows = (new Flow())->process(
            new Rows(
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
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
                Row::create(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                Row::create(int_entry('id', 2), null_entry('name'), bool_entry('active', false)),
                Row::create(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ),
            $rows
        );
    }

    public function test_until() : void
    {
        $rows = (new Flow())
            ->read(from_all(
                from_array([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                    ['id' => 4],
                    ['id' => 5],
                ]),
                from_array([
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
                Row::create(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                Row::create(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                Row::create(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                Row::create(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                Row::create(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                Row::create(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                Row::create(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
            )
        )
            ->rename('country', 'country_code')
            ->void()
            ->aggregate(average(ref('age')))
            ->rename('age_avg', 'average_age')
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
}
