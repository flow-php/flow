<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{average,
    bool_entry,
    compare_entries_by_name_desc,
    df,
    float_entry,
    from_all,
    from_array,
    from_rows,
    int_entry,
    json_entry,
    lit,
    ref,
    refs,
    str_entry,
    string_entry,
    to_callable};
use function Flow\ETL\DSL\{bool_schema, boolean_entry, integer_entry, integer_schema, schema, string_schema};
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Row\Entry\{DateTimeEntry};
use Flow\ETL\Row\Schema\SelectiveValidator;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\{DataFrame,
    Extractor,
    FlowContext,
    Loader,
    Row,
    Rows,
    Tests\FlowTestCase,
    Transformation,
    Transformer};
use PHPUnit\Framework\{Assert};

final class DataFrameTest extends FlowTestCase
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

        self::assertEquals(
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

        self::assertSame(5, $count);
    }

    public function test_drop() : void
    {
        $rows = df()->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
        )
            ->drop('id')
            ->fetch();

        self::assertEquals(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(str_entry('name', 'bar'), bool_entry('active', false))),
            $rows
        );
    }

    public function test_drop_duplicates() : void
    {
        $rows = df()->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
        )
            ->dropDuplicates(ref('id'))
            ->fetch();

        self::assertEquals(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false))),
            $rows
        );
    }

    public function test_encapsulate_transformations() : void
    {
        $rows = df()->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')), \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')), \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30), str_entry('gender', 'female')), \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'female')), \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')), \Flow\ETL\DSL\row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45), str_entry('gender', 'female')), \Flow\ETL\DSL\row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')))
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

        self::assertEquals(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(str_entry('country', 'pl'), int_entry('age', 2)), \Flow\ETL\DSL\row(str_entry('country', 'pl'), int_entry('age', 2)), \Flow\ETL\DSL\row(str_entry('country', 'pl'), float_entry('age', 2.5)), \Flow\ETL\DSL\row(str_entry('country', 'pl'), int_entry('age', 3)), \Flow\ETL\DSL\row(str_entry('country', 'us'), int_entry('age', 4)), \Flow\ETL\DSL\row(str_entry('country', 'us'), int_entry('age', 4)), \Flow\ETL\DSL\row(str_entry('country', 'us'), float_entry('age', 4.5)), \Flow\ETL\DSL\row(str_entry('country', 'us'), int_entry('age', 5))),
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
                        yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', $i)));
                    }
                }
            }
        )
            ->filter(ref('id')->mod(lit(2))->same(lit(0)))
            ->fetch();

        self::assertCount(5, $rows);
        self::assertSame(
            [['id' => 2], ['id' => 4], ['id' => 6], ['id' => 8], ['id' => 10]],
            $rows->toArray()
        );
    }

    public function test_foreach() : void
    {
        df()->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
        )
            ->foreach(function (Rows $rows) : void {
                $this->assertEquals(
                    \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false))),
                    $rows
                );
            });
    }

    public function test_get() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('name', 'baz')), \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('name', 'baz')))
            ))
            ->get();

        self::assertEquals([$extractedRows], \iterator_to_array($rows));
    }

    public function test_get_as_array() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('name', 'baz')), \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('name', 'baz')))
            ))
            ->getAsArray();

        self::assertEquals([
            $extractedRows->toArray(),
        ], \iterator_to_array($rows));
    }

    public function test_get_each() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('name', 'baz')), \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('name', 'baz')))
            ))
            ->getEach();

        self::assertEquals([
            \Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo')),
            \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar')),
            \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('name', 'baz')),
            \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('name', 'foo')),
            \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('name', 'bar')),
            \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('name', 'baz')),
        ], \iterator_to_array($rows));
    }

    public function test_get_each_as_array() : void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('name', 'baz')), \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('name', 'foo')), \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('name', 'bar')), \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('name', 'baz')))
            ))
            ->getEachAsArray();

        self::assertEquals(
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
        $rows = (data_frame())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', $i)));
                    }
                }
            }
        )
            ->map(fn (Row $row) => $row->add(boolean_entry('odd', $row->valueOf('id') % 2 === 0)))
            ->fetch();

        self::assertCount(10, $rows);
        self::assertSame(
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

    public function test_order_entries() : void
    {
        $dataset1 = [
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
        ];

        $df = df()
            ->read(from_array($dataset1))
            ->autoCast()
            ->reorderEntries(compare_entries_by_name_desc());

        self::assertEquals(
            ['name', 'id', 'active'],
            \array_keys($df->fetch()[0]->toArray())
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
                yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 101), boolean_entry('deleted', false), new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')), string_entry('phase', null)));

                yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 102), boolean_entry('deleted', true), new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-25')), string_entry('phase', null)));
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context) : Rows
            {
                return $rows->map(
                    fn (Row $row) : Row => $row->set(string_entry('stamp', 'zero'))
                );
            }
        };

        $loader = new class implements Loader {
            public array $result = [];

            public function load(Rows $rows, FlowContext $context) : void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        (data_frame())->read($extractor)
            ->onError(new IgnoreError())
            ->rows($addStampStringEntry)
            ->rows(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context) : Rows
                {
                    throw new \RuntimeException('Unexpected exception');
                }
            })
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'one'))
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'two'))
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'three'))
            ->write($loader)
            ->run();

        self::assertEquals(
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
        $collectedRows = (data_frame())->process(
            $rows = \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 101), boolean_entry('deleted', false), new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')), string_entry('phase', null)))
        )
            ->fetch();

        self::assertEquals($rows, $collectedRows);
    }

    public function test_select() : void
    {
        $rows = (data_frame())->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
        )
            ->select('name', 'id')
            ->fetch();

        self::assertEquals(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(str_entry('name', 'foo'), int_entry('id', 1)), \Flow\ETL\DSL\row(str_entry('name', null), int_entry('id', 2)), \Flow\ETL\DSL\row(str_entry('name', 'bar'), int_entry('id', 2))),
            $rows
        );
    }

    public function test_selective_validation_against_schema() : void
    {
        $rows = (data_frame())->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), json_entry('tags', ['foo', 'bar'])), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
        )->validate(
            schema(integer_schema('id', $nullable = false)),
            new SelectiveValidator()
        )->fetch();

        self::assertEquals(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), json_entry('tags', ['foo', 'bar'])), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false))),
            $rows
        );
    }

    public function test_strict_validation_against_schema() : void
    {
        $rows = (data_frame())->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)))
        )->validate(
            schema(integer_schema('id', $nullable = false), string_schema('name', $nullable = true), bool_schema('active', $nullable = false))
        )->fetch();

        self::assertEquals(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false))),
            $rows
        );
    }

    public function test_until() : void
    {
        $rows = (data_frame())
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

        self::assertCount(3, $rows);
        self::assertSame(
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
        $rows = (data_frame())->process(
            \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)), \Flow\ETL\DSL\row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)), \Flow\ETL\DSL\row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)), \Flow\ETL\DSL\row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)), \Flow\ETL\DSL\row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)), \Flow\ETL\DSL\row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)), \Flow\ETL\DSL\row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)), \Flow\ETL\DSL\row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)))
        )
            ->rename('country', 'country_code')
            ->void()
            ->aggregate(average(ref('age')))
            ->rename('age_avg', 'average_age')
            ->fetch();

        self::assertEquals(
            \Flow\ETL\DSL\rows(),
            $rows
        );
    }

    public function test_with_batch_size() : void
    {
        (data_frame())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 1)), \Flow\ETL\DSL\row(integer_entry('id', 2)), \Flow\ETL\DSL\row(integer_entry('id', 3)), \Flow\ETL\DSL\row(integer_entry('id', 4)), \Flow\ETL\DSL\row(integer_entry('id', 5)), \Flow\ETL\DSL\row(integer_entry('id', 6)), \Flow\ETL\DSL\row(integer_entry('id', 7)), \Flow\ETL\DSL\row(integer_entry('id', 8)), \Flow\ETL\DSL\row(integer_entry('id', 9)), \Flow\ETL\DSL\row(integer_entry('id', 10)));
                }
            }
        )
            ->with(
                new class implements Transformer {
                    public function transform(Rows $rows, FlowContext $context) : Rows
                    {
                        return $rows->map(fn (Row $row) => $row->rename('id', 'new_id'));
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
                }
            )
            ->run();
    }

    public function test_with_collecting() : void
    {
        (data_frame())->extract(
            new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context) : \Generator
                {
                    yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 1)));
                    yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 2)));
                    yield \Flow\ETL\DSL\rows(\Flow\ETL\DSL\row(integer_entry('id', 3)));
                }
            }
        )
            ->with(
                new class implements Transformer {
                    public function transform(Rows $rows, FlowContext $context) : Rows
                    {
                        return $rows->map(fn (Row $row) => $row->rename('id', 'new_id'));
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
                }
            )
            ->run();
    }
}
