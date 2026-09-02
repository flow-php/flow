<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row\RowRenaming;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\SelectiveValidator;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Generator;
use PHPUnit\Framework\Assert;
use RuntimeException;

use function array_merge;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_json;
use function iterator_to_array;

final class DataFrameTest extends FlowTestCase
{
    public function test_batch_size(): void
    {
        $spy = new SpyLoader();

        df()
            ->read(from_array([
                ['id' => '01', 'elements' => [['sub_id' => '01_01'], ['sub_id' => '01_02']]],
                ['id' => '02', 'elements' => [['sub_id' => '02_01'], ['sub_id' => '02_02']]],
                ['id' => '03', 'elements' => [['sub_id' => '03_01'], ['sub_id' => '03_02']]],
                ['id' => '04', 'elements' => [['sub_id' => '04_01'], ['sub_id' => '04_02']]],
                ['id' => '05', 'elements' => [['sub_id' => '05_01'], ['sub_id' => '05_02'], ['sub_id' => '05_03']]],
            ]))
            ->batchSize(1)
            ->load($spy)
            ->withEntry('element', ref('elements')->expand())
            ->batchSize(3)
            ->run(function (Rows $rows): void {
                $this->assertLessThanOrEqual(3, $rows->count());
            });

        static::assertSame([1, 1, 1, 1, 1], $spy->loadedRowCounts());
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
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ))
            ->drop('id')
            ->fetch();

        static::assertEquals(
            rows(
                schema(str_schema('name', nullable: true), bool_schema('active')),
                row(['name' => 'foo', 'active' => true]),
                row(['name' => null, 'active' => false]),
                row(['name' => 'bar', 'active' => false]),
            ),
            $rows,
        );
    }

    public function test_drop_duplicates(): void
    {
        $rows = df()
            ->process(rows(
                schema(int_schema('id'), str_schema('name'), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ))
            ->dropDuplicates(ref('id'))
            ->fetch();

        static::assertEquals(
            rows(
                schema(int_schema('id'), str_schema('name'), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ),
            $rows,
        );
    }

    public function test_encapsulate_transformations(): void
    {
        $rows = df()
            ->process(rows(
                schema(int_schema('id'), str_schema('country'), int_schema('age'), str_schema('gender')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 2, 'country' => 'PL', 'age' => 20, 'gender' => 'male']),
                row(['id' => 3, 'country' => 'PL', 'age' => 25, 'gender' => 'male']),
                row(['id' => 4, 'country' => 'PL', 'age' => 30, 'gender' => 'female']),
                row(['id' => 5, 'country' => 'US', 'age' => 40, 'gender' => 'female']),
                row(['id' => 6, 'country' => 'US', 'age' => 40, 'gender' => 'male']),
                row(['id' => 7, 'country' => 'US', 'age' => 45, 'gender' => 'female']),
                row(['id' => 9, 'country' => 'US', 'age' => 50, 'gender' => 'male']),
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
                schema(str_schema('country'), float_schema('age')),
                row(['country' => 'pl', 'age' => 2.0]),
                row(['country' => 'pl', 'age' => 2.0]),
                row(['country' => 'pl', 'age' => 2.5]),
                row(['country' => 'pl', 'age' => 3.0]),
                row(['country' => 'us', 'age' => 4.0]),
                row(['country' => 'us', 'age' => 4.0]),
                row(['country' => 'us', 'age' => 4.5]),
                row(['country' => 'us', 'age' => 5.0]),
            ),
            $rows,
        );
    }

    public function test_filter(): void
    {
        $rows = df()
            ->extract(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i]));
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
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ))
            ->foreach(function (Rows $rows): void {
                $this->assertEquals(
                    rows(
                        schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                        row(['id' => 1, 'name' => 'foo', 'active' => true]),
                        row(['id' => 2, 'name' => null, 'active' => false]),
                        row(['id' => 2, 'name' => 'bar', 'active' => false]),
                    ),
                    $rows,
                );
            });
    }

    public function test_get(): void
    {
        $rows = df()->read(from_rows(
            $extractedRows = rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'foo']),
                row(['id' => 2, 'name' => 'bar']),
                row(['id' => 3, 'name' => 'baz']),
                row(['id' => 4, 'name' => 'foo']),
                row(['id' => 5, 'name' => 'bar']),
                row(['id' => 6, 'name' => 'baz']),
            ),
        ))->get();

        static::assertEquals([$extractedRows], iterator_to_array($rows));
    }

    public function test_get_as_array(): void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = rows(
                    schema(int_schema('id'), str_schema('name')),
                    row(['id' => 1, 'name' => 'foo']),
                    row(['id' => 2, 'name' => 'bar']),
                    row(['id' => 3, 'name' => 'baz']),
                    row(['id' => 4, 'name' => 'foo']),
                    row(['id' => 5, 'name' => 'bar']),
                    row(['id' => 6, 'name' => 'baz']),
                ),
            ))
            ->getAsArray();

        static::assertEquals(
            [
                $extractedRows->toArray(),
            ],
            iterator_to_array($rows),
        );
    }

    public function test_get_each(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'foo']),
                row(['id' => 2, 'name' => 'bar']),
                row(['id' => 3, 'name' => 'baz']),
                row(['id' => 4, 'name' => 'foo']),
                row(['id' => 5, 'name' => 'bar']),
                row(['id' => 6, 'name' => 'baz']),
            )))
            ->getEach();

        static::assertEquals(
            [
                row(['id' => 1, 'name' => 'foo']),
                row(['id' => 2, 'name' => 'bar']),
                row(['id' => 3, 'name' => 'baz']),
                row(['id' => 4, 'name' => 'foo']),
                row(['id' => 5, 'name' => 'bar']),
                row(['id' => 6, 'name' => 'baz']),
            ],
            iterator_to_array($rows),
        );
    }

    public function test_get_each_as_array(): void
    {
        $rows = df()
            ->read(from_rows(rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'foo']),
                row(['id' => 2, 'name' => 'bar']),
                row(['id' => 3, 'name' => 'baz']),
                row(['id' => 4, 'name' => 'foo']),
                row(['id' => 5, 'name' => 'bar']),
                row(['id' => 6, 'name' => 'baz']),
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
            iterator_to_array($rows),
        );
    }

    public function test_deriving_a_boolean_entry_from_an_existing_column(): void
    {
        $rows = data_frame()
            ->extract(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i]));
                    }
                }
            })
            ->withEntry('odd', ref('id')->mod(lit(2))->equals(lit(0)))
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

    public function test_pipeline(): void
    {
        $extractor = new class implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return new Schema();
            }

            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context): Generator
            {
                yield rows(
                    schema(
                        int_schema('id'),
                        bool_schema('deleted'),
                        datetime_schema('expiration-date'),
                        str_schema('phase', nullable: true),
                    ),
                    row([
                        'id' => 101,
                        'deleted' => false,
                        'expiration-date' => new DateTimeImmutable('2020-08-24'),
                        'phase' => null,
                    ]),
                );

                yield rows(
                    schema(
                        int_schema('id'),
                        bool_schema('deleted'),
                        datetime_schema('expiration-date'),
                        str_schema('phase', nullable: true),
                    ),
                    row([
                        'id' => 102,
                        'deleted' => true,
                        'expiration-date' => new DateTimeImmutable('2020-08-25'),
                        'phase' => null,
                    ]),
                );
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context): Rows
            {
                $stamped = [];

                foreach ($rows->all() as $row) {
                    $stamped[] = row([...$row->values(), 'stamp' => 'zero']);
                }

                return new Rows($rows->schema()->add(str_schema('stamp')), ...$stamped);
            }
        };

        $loader = new class implements Loader {
            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, FlowContext $context): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()
            ->read($extractor)
            ->onError(new IgnoreError())
            ->rows($addStampStringEntry)
            ->rows(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context): Rows
                {
                    throw new RuntimeException('Unexpected exception');
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
                    'expiration-date' => new DateTimeImmutable('2020-08-24'),
                    'phase' => null,
                ],
                [
                    'id' => 102,
                    'stamp' => 'zero:one:two:three',
                    'deleted' => true,
                    'expiration-date' => new DateTimeImmutable('2020-08-25'),
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
                $rows = rows(
                    schema(
                        int_schema('id'),
                        bool_schema('deleted'),
                        datetime_schema('expiration-date'),
                        str_schema('phase', nullable: true),
                    ),
                    row([
                        'id' => 101,
                        'deleted' => false,
                        'expiration-date' => new DateTimeImmutable('2020-08-24'),
                        'phase' => null,
                    ]),
                ),
            )
            ->fetch();

        static::assertEquals($rows, $collectedRows);
    }

    public function test_select(): void
    {
        $rows = data_frame()
            ->process(rows(
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ))
            ->select('name', 'id')
            ->fetch();

        static::assertEquals(
            rows(
                schema(str_schema('name', nullable: true), int_schema('id')),
                row(['name' => 'foo', 'id' => 1]),
                row(['name' => null, 'id' => 2]),
                row(['name' => 'bar', 'id' => 2]),
            ),
            $rows,
        );
    }

    public function test_selective_validation_against_schema(): void
    {
        $rows = data_frame()
            ->process(rows(
                schema(
                    int_schema('id'),
                    str_schema('name', nullable: true),
                    bool_schema('active', nullable: true),
                    json_schema('tags', nullable: true),
                ),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'tags' => type_json()->cast(['foo', 'bar'])]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ))
            ->match(schema(integer_schema('id', false)), new SelectiveValidator())
            ->fetch();

        static::assertSame(
            rows(
                schema(
                    int_schema('id'),
                    str_schema('name', nullable: true),
                    bool_schema('active', nullable: true),
                    json_schema('tags', nullable: true),
                ),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'tags' => type_json()->cast(['foo', 'bar'])]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            )->toArray(),
            $rows->toArray(),
        );
    }

    public function test_sort_by_accepts_a_column_name_string(): void
    {
        static::assertSame(
            [1, 2, 3],
            df()
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
                ->sortBy('id')
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_sort_by_accepts_column_name_strings_inside_the_array_form(): void
    {
        static::assertSame(
            [['name' => 'a', 'id' => 1], ['name' => 'a', 'id' => 2], ['name' => 'b', 'id' => 1]],
            df()
                ->read(from_array([
                    ['name' => 'b', 'id' => 1],
                    ['name' => 'a', 'id' => 2],
                    ['name' => 'a', 'id' => 1],
                ]))
                ->sortBy(['name', ref('id')])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_strict_validation_against_schema(): void
    {
        $rows = data_frame()
            ->process(rows(
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
            ))
            ->match(schema(integer_schema('id', false), string_schema('name', true), bool_schema('active', false)))
            ->fetch();

        static::assertSame(
            rows(
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                row(['id' => 1, 'name' => 'foo', 'active' => true]),
                row(['id' => 2, 'name' => null, 'active' => false]),
                row(['id' => 2, 'name' => 'bar', 'active' => false]),
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
                schema(int_schema('id'), str_schema('country'), int_schema('age')),
                row(['id' => 1, 'country' => 'PL', 'age' => 20]),
                row(['id' => 2, 'country' => 'PL', 'age' => 20]),
                row(['id' => 3, 'country' => 'PL', 'age' => 25]),
                row(['id' => 4, 'country' => 'PL', 'age' => 30]),
                row(['id' => 5, 'country' => 'US', 'age' => 40]),
                row(['id' => 6, 'country' => 'US', 'age' => 40]),
                row(['id' => 7, 'country' => 'US', 'age' => 45]),
                row(['id' => 9, 'country' => 'US', 'age' => 50]),
            ))
            ->rename('country', 'country_code')
            ->void()
            ->aggregate([average(ref('age'))])
            ->rename('age_avg', 'average_age')
            ->fetch();

        static::assertEquals(rows(schema()), $rows);
    }

    public function test_with_batch_size(): void
    {
        data_frame()
            ->extract(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    yield rows(
                        schema(integer_schema('id')),
                        row(['id' => 1]),
                        row(['id' => 2]),
                        row(['id' => 3]),
                        row(['id' => 4]),
                        row(['id' => 5]),
                        row(['id' => 6]),
                        row(['id' => 7]),
                        row(['id' => 8]),
                        row(['id' => 9]),
                        row(['id' => 10]),
                    );
                }
            })
            ->with(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context): Rows
                {
                    $renamed = [];

                    foreach ($rows->all() as $row) {
                        $renamed[] = RowRenaming::of(['id' => 'new_id'])->apply($row);
                    }

                    return new Rows($rows->schema()->rename('id', 'new_id'), ...$renamed);
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
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return new Schema();
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    yield rows(schema(integer_schema('id')), row(['id' => 1]));
                    yield rows(schema(integer_schema('id')), row(['id' => 2]));
                    yield rows(schema(integer_schema('id')), row(['id' => 3]));
                }
            })
            ->with(new class implements Transformer {
                public function transform(Rows $rows, FlowContext $context): Rows
                {
                    $renamed = [];

                    foreach ($rows->all() as $row) {
                        $renamed[] = RowRenaming::of(['id' => 'new_id'])->apply($row);
                    }

                    return new Rows($rows->schema()->rename('id', 'new_id'), ...$renamed);
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
