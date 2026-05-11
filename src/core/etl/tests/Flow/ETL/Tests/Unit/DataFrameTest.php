<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Validator\SelectiveValidator;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use PHPUnit\Framework\Assert;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\boolean_entry;
use function Flow\ETL\DSL\compare_entries_by_name_desc;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\to_callable;
use function Flow\Types\DSL\type_integer;

final class DataFrameTest extends FlowTestCase
{
    public function test_batch_size(): void
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
            ->load(to_callable(function (Rows $rows): void {
                $this->assertCount(1, $rows);
            }))
            ->withEntry('element', ref('elements')->expand())
            ->batchSize(3)
            ->run(function (Rows $rows): void {
                $this->assertLessThanOrEqual(3, $rows->count());
            });
    }

    public function test_collect_references(): void
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
            ->read(from_all(from_array($dataset1), from_array($dataset2)))
            ->collectRefs($refs = refs())
            ->run();

        static::assertEquals(refs('id', 'name', 'active', 'country', 'group'), $refs);
    }

    public function test_count(): void
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

        static::assertSame(5, $count);
    }

    public function test_drop(): void
    {
        $rows = df()
            ->process(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ))
            ->drop('id')
            ->fetch();

        static::assertEquals(
            rows(
                row(str_entry('name', 'foo'), bool_entry('active', true)),
                row(str_entry('name', null), bool_entry('active', false)),
                row(str_entry('name', 'bar'), bool_entry('active', false)),
            ),
            $rows,
        );
    }

    public function test_drop_duplicates(): void
    {
        $rows = df()
            ->process(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ))
            ->dropDuplicates(ref('id'))
            ->fetch();

        static::assertEquals(
            rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ),
            $rows,
        );
    }

    public function test_encapsulate_transformations(): void
    {
        $rows = df()
            ->process(rows(
                row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20), str_entry('gender', 'male')),
                row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25), str_entry('gender', 'male')),
                row(
                    int_entry('id', 4),
                    str_entry('country', 'PL'),
                    int_entry('age', 30),
                    str_entry('gender', 'female'),
                ),
                row(
                    int_entry('id', 5),
                    str_entry('country', 'US'),
                    int_entry('age', 40),
                    str_entry('gender', 'female'),
                ),
                row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40), str_entry('gender', 'male')),
                row(
                    int_entry('id', 7),
                    str_entry('country', 'US'),
                    int_entry('age', 45),
                    str_entry('gender', 'female'),
                ),
                row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50), str_entry('gender', 'male')),
            ))
            ->rows(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->withEntry('country', ref('country')->lower())->withEntry('age', ref(
                        'age',
                    )->divide(lit(10), scale: 2));
                }
            })
            ->rows(new class implements Transformation {
                public function transform(DataFrame $dataFrame): DataFrame
                {
                    return $dataFrame->drop('gender')->drop('id');
                }
            })
            ->fetch();

        static::assertEquals(
            rows(
                row(str_entry('country', 'pl'), int_entry('age', 2)),
                row(str_entry('country', 'pl'), int_entry('age', 2)),
                row(str_entry('country', 'pl'), float_entry('age', 2.5)),
                row(str_entry('country', 'pl'), int_entry('age', 3)),
                row(str_entry('country', 'us'), int_entry('age', 4)),
                row(str_entry('country', 'us'), int_entry('age', 4)),
                row(str_entry('country', 'us'), float_entry('age', 4.5)),
                row(str_entry('country', 'us'), int_entry('age', 5)),
            ),
            $rows,
        );
    }

    public function test_filter(): void
    {
        $rows = df()
            ->extract(new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield rows(row(integer_entry('id', $i)));
                    }
                }
            })
            ->filter(ref('id')->mod(lit(2))->same(lit(0)))
            ->fetch();

        static::assertCount(5, $rows);
        static::assertSame([['id' => 2], ['id' => 4], ['id' => 6], ['id' => 8], ['id' => 10]], $rows->toArray());
    }

    public function test_foreach(): void
    {
        df()
            ->process(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ))
            ->foreach(function (Rows $rows): void {
                $this->assertEquals(
                    rows(
                        row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                        row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                        row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
                    ),
                    $rows,
                );
            });
    }

    public function test_get(): void
    {
        $rows = df()->read(from_rows(
            $extractedRows = rows(
                row(int_entry('id', 1), str_entry('name', 'foo')),
                row(int_entry('id', 2), str_entry('name', 'bar')),
                row(int_entry('id', 3), str_entry('name', 'baz')),
                row(int_entry('id', 4), str_entry('name', 'foo')),
                row(int_entry('id', 5), str_entry('name', 'bar')),
                row(int_entry('id', 6), str_entry('name', 'baz')),
            ),
        ))->get();

        static::assertEquals([$extractedRows], \iterator_to_array($rows));
    }

    public function test_get_as_array(): void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = rows(
                    row(int_entry('id', 1), str_entry('name', 'foo')),
                    row(int_entry('id', 2), str_entry('name', 'bar')),
                    row(int_entry('id', 3), str_entry('name', 'baz')),
                    row(int_entry('id', 4), str_entry('name', 'foo')),
                    row(int_entry('id', 5), str_entry('name', 'bar')),
                    row(int_entry('id', 6), str_entry('name', 'baz')),
                ),
            ))
            ->getAsArray();

        static::assertEquals(
            [
                $extractedRows->toArray(),
            ],
            \iterator_to_array($rows),
        );
    }

    public function test_get_each(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), str_entry('name', 'foo')),
                row(int_entry('id', 2), str_entry('name', 'bar')),
                row(int_entry('id', 3), str_entry('name', 'baz')),
                row(int_entry('id', 4), str_entry('name', 'foo')),
                row(int_entry('id', 5), str_entry('name', 'bar')),
                row(int_entry('id', 6), str_entry('name', 'baz')),
            )))
            ->getEach();

        static::assertEquals(
            [
                row(int_entry('id', 1), str_entry('name', 'foo')),
                row(int_entry('id', 2), str_entry('name', 'bar')),
                row(int_entry('id', 3), str_entry('name', 'baz')),
                row(int_entry('id', 4), str_entry('name', 'foo')),
                row(int_entry('id', 5), str_entry('name', 'bar')),
                row(int_entry('id', 6), str_entry('name', 'baz')),
            ],
            \iterator_to_array($rows),
        );
    }

    public function test_get_each_as_array(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                row(int_entry('id', 1), str_entry('name', 'foo')),
                row(int_entry('id', 2), str_entry('name', 'bar')),
                row(int_entry('id', 3), str_entry('name', 'baz')),
                row(int_entry('id', 4), str_entry('name', 'foo')),
                row(int_entry('id', 5), str_entry('name', 'bar')),
                row(int_entry('id', 6), str_entry('name', 'baz')),
            )))
            ->getEachAsArray();

        static::assertEquals(
            [
                ['id' => 1, 'name' => 'foo'],
                ['id' => 2, 'name' => 'bar'],
                ['id' => 3, 'name' => 'baz'],
                ['id' => 4, 'name' => 'foo'],
                ['id' => 5, 'name' => 'bar'],
                ['id' => 6, 'name' => 'baz'],
            ],
            \iterator_to_array($rows),
        );
    }

    public function test_map(): void
    {
        $rows = data_frame()
            ->extract(new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield rows(row(integer_entry('id', $i)));
                    }
                }
            })
            ->map(static fn(Row $row) => $row->add(boolean_entry(
                'odd',
                (type_integer()->assert($row->valueOf('id')) % 2) === 0,
            )))
            ->fetch();

        static::assertCount(10, $rows);
        static::assertSame(
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
            $rows->toArray(),
        );
    }

    public function test_order_entries(): void
    {
        $dataset1 = [
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
            ['id' => 1, 'name' => 'test', 'active' => false],
        ];

        $df = df()->read(from_array($dataset1))->autoCast()->reorderEntries(compare_entries_by_name_desc());

        static::assertEquals(['name', 'id', 'active'], \array_keys($df->fetch()[0]->toArray()));
    }

    public function test_pipeline(): void
    {
        $extractor = new class implements Extractor {
            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context): \Generator
            {
                yield rows(row(
                    integer_entry('id', 101),
                    boolean_entry('deleted', false),
                    new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                    string_entry('phase', null),
                ));

                yield rows(row(
                    integer_entry('id', 102),
                    boolean_entry('deleted', true),
                    new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-25')),
                    string_entry('phase', null),
                ));
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context): Rows
            {
                return $rows->map(static fn(Row $row): Row => $row->set(string_entry('stamp', 'zero')));
            }
        };

        $loader = new class implements Loader {
            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->result = \array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()
            ->read($extractor)
            ->onError(new IgnoreError())
            ->rows($addStampStringEntry)
            ->rows(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context): Rows
                {
                    throw new \RuntimeException('Unexpected exception');
                }
            })
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'one'))
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'two'))
            ->rows(AddStampToStringEntryTransformer::divideBySemicolon('stamp', 'three'))
            ->write($loader)
            ->run();

        static::assertEquals(
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

    public function test_process_constructor(): void
    {
        $collectedRows = data_frame()
            ->process(
                $rows = rows(row(
                    integer_entry('id', 101),
                    boolean_entry('deleted', false),
                    new DateTimeEntry('expiration-date', new \DateTimeImmutable('2020-08-24')),
                    string_entry('phase', null),
                )),
            )
            ->fetch();

        static::assertEquals($rows, $collectedRows);
    }

    public function test_select(): void
    {
        $rows = data_frame()
            ->process(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ))
            ->select('name', 'id')
            ->fetch();

        static::assertEquals(
            rows(
                row(str_entry('name', 'foo'), int_entry('id', 1)),
                row(str_entry('name', null), int_entry('id', 2)),
                row(str_entry('name', 'bar'), int_entry('id', 2)),
            ),
            $rows,
        );
    }

    public function test_selective_validation_against_schema(): void
    {
        $rows = data_frame()
            ->process(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), json_entry('tags', ['foo', 'bar'])),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ))
            ->match(schema(integer_schema('id', false)), new SelectiveValidator())
            ->fetch();

        static::assertSame(
            rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), json_entry('tags', ['foo', 'bar'])),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )->toArray(),
            $rows->toArray(),
        );
    }

    public function test_strict_validation_against_schema(): void
    {
        $rows = data_frame()
            ->process(rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            ))
            ->match(schema(integer_schema('id', false), string_schema('name', true), bool_schema('active', false)))
            ->fetch();

        static::assertSame(
            rows(
                row(int_entry('id', 1), str_entry('name', 'foo'), bool_entry('active', true)),
                row(int_entry('id', 2), str_entry('name', null), bool_entry('active', false)),
                row(int_entry('id', 2), str_entry('name', 'bar'), bool_entry('active', false)),
            )->toArray(),
            $rows->toArray(),
        );
    }

    public function test_until(): void
    {
        $rows = data_frame()
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
                ]),
            ))
            ->until(ref('id')->lessThanEqual(lit(3)))
            ->fetch();

        static::assertCount(3, $rows);
        static::assertSame(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $rows->toArray(),
        );
    }

    public function test_void(): void
    {
        $rows = data_frame()
            ->process(rows(
                row(int_entry('id', 1), str_entry('country', 'PL'), int_entry('age', 20)),
                row(int_entry('id', 2), str_entry('country', 'PL'), int_entry('age', 20)),
                row(int_entry('id', 3), str_entry('country', 'PL'), int_entry('age', 25)),
                row(int_entry('id', 4), str_entry('country', 'PL'), int_entry('age', 30)),
                row(int_entry('id', 5), str_entry('country', 'US'), int_entry('age', 40)),
                row(int_entry('id', 6), str_entry('country', 'US'), int_entry('age', 40)),
                row(int_entry('id', 7), str_entry('country', 'US'), int_entry('age', 45)),
                row(int_entry('id', 9), str_entry('country', 'US'), int_entry('age', 50)),
            ))
            ->rename('country', 'country_code')
            ->void()
            ->aggregate(average(ref('age')))
            ->rename('age_avg', 'average_age')
            ->fetch();

        static::assertEquals(rows(), $rows);
    }

    public function test_with_batch_size(): void
    {
        data_frame()
            ->extract(new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    yield rows(
                        row(integer_entry('id', 1)),
                        row(integer_entry('id', 2)),
                        row(integer_entry('id', 3)),
                        row(integer_entry('id', 4)),
                        row(integer_entry('id', 5)),
                        row(integer_entry('id', 6)),
                        row(integer_entry('id', 7)),
                        row(integer_entry('id', 8)),
                        row(integer_entry('id', 9)),
                        row(integer_entry('id', 10)),
                    );
                }
            })
            ->with(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context): Rows
                {
                    return $rows->map(static fn(Row $row) => $row->rename('id', 'new_id'));
                }
            })
            ->batchSize(2)
            ->load(new class implements Loader {
                public function load(Rows $rows, FlowContext $context): void
                {
                    Assert::assertCount(2, $rows);
                }
            })
            ->run();
    }

    public function test_with_collecting(): void
    {
        data_frame()
            ->extract(new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    yield rows(row(integer_entry('id', 1)));
                    yield rows(row(integer_entry('id', 2)));
                    yield rows(row(integer_entry('id', 3)));
                }
            })
            ->with(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context): Rows
                {
                    return $rows->map(static fn(Row $row) => $row->rename('id', 'new_id'));
                }
            })
            ->collect()
            ->load(new class implements Loader {
                public function load(Rows $rows, FlowContext $context): void
                {
                    Assert::assertCount(3, $rows);
                }
            })
            ->run();
    }
}
