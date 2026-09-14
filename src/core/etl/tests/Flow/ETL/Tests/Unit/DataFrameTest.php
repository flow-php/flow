<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use ArrayObject;
use DateTimeImmutable;
use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Plan\Explain;
use Flow\ETL\Plan\Node;
use Flow\ETL\Planner;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Row\RowRenaming;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Validator\SelectiveValidator;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\RecordingScanExtractor;
use Flow\ETL\Tests\Double\RowLessExtractor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Generator;
use PHPUnit\Framework\Assert;
use RuntimeException;

use function array_merge;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\to_memory;
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
                    return schema(integer_schema('id'));
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
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
                    return schema(integer_schema('id'));
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
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
                return schema(
                    int_schema('id'),
                    bool_schema('deleted'),
                    datetime_schema('expiration-date'),
                    str_schema('phase', nullable: true),
                );
            }

            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, Signal|null, void>
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
            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input->add(str_schema('stamp')));
            }

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
                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input);
                }

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

        // IgnoreError drops a batch whose transformation failed, so the stamps after it never run
        static::assertSame([], $loader->result);
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

        // void() drops rows, not columns - the batch it yields carries the plan's declared schema, and
        // the global aggregate that follows it reads zero rows, so it emits its initial accumulators
        static::assertEquals(
            rows(schema(float_schema('average_age', nullable: true)), row(['average_age' => null])),
            $rows,
        );
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
                    return schema(integer_schema('id'));
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
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
                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input->rename('id', 'new_id'));
                }

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
                    return schema(integer_schema('id'));
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(FlowContext $context): Generator
                {
                    yield rows(schema(integer_schema('id')), row(['id' => 1]));
                    yield rows(schema(integer_schema('id')), row(['id' => 2]));
                    yield rows(schema(integer_schema('id')), row(['id' => 3]));
                }
            })
            ->with(new class implements Transformer {
                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input->rename('id', 'new_id'));
                }

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

    public function test_fetch_carries_the_source_schema_when_the_source_yields_no_rows(): void
    {
        $extractor = new RowLessExtractor(schema(int_schema('id', true), str_schema('name', true)));

        $rows = data_frame()->extract($extractor)->fetch();

        static::assertCount(0, $rows);
        static::assertTrue($rows->schema()->isSame(schema(int_schema('id', true), str_schema('name', true))));
    }

    public function test_fetch_does_not_run_the_pipeline_twice_when_the_source_yields_no_rows(): void
    {
        $extractor = new RowLessExtractor(schema(int_schema('id', true)));

        data_frame()->extract($extractor)->fetch();

        static::assertSame(1, $extractor->extractCalls);
    }

    public function test_fetch_is_empty_when_a_row_less_source_cannot_describe_itself(): void
    {
        $rows = data_frame()->extract(new UndescribableRowLessExtractor())->fetch();

        static::assertCount(0, $rows);
        static::assertCount(0, $rows->schema()->definitions());
    }

    /**
     * A step may add, drop or retype columns, so the source schema is no longer the output schema. Answering this
     * case needs the output schema computed without executing, which is not built yet.
     */
    public function test_fetch_does_not_claim_the_source_schema_when_a_step_reshapes_the_rows(): void
    {
        $rows = data_frame()
            ->extract(new RowLessExtractor(schema(int_schema('id', true))))
            ->drop('id')
            ->fetch();

        static::assertCount(0, $rows);
        static::assertCount(0, $rows->schema()->definitions());
    }

    public function test_a_pushed_limit_travels_in_the_scan_and_leaves_the_users_extractor_alone(): void
    {
        $extractor = new RecordingScanExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        $frame = df()
            ->read($extractor)
            ->withEntry('doubled', ref('id')->multiply(lit(2)))
            ->limit(1);

        $frame->schema();

        static::assertSame([['id' => 1, 'doubled' => 2]], $frame->fetch()->toArray());
        static::assertSame($extractor, $frame->extractor());
        static::assertCount(1, $extractor->scans);
        static::assertSame(1, $extractor->scans[0]->limit);
    }

    public function test_a_limit_inside_a_joins_right_side_is_pushed_into_that_source(): void
    {
        $extractor = new RecordingScanExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        $right = df()->read($extractor)->limit(1);

        df()
            ->read(from_array([['id' => 1]]))
            ->join($right, join_on(['id' => 'id'], 'r_'))
            ->fetch();

        static::assertSame(1, $extractor->scans[0]->limit);
    }

    public function test_an_empty_fetch_plans_once(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();

        $rows = df(config_builder()->planner(new Planner(Lowerings::default(), new RecordingRule('plan', $log))))
            ->read(from_array([['id' => 1]]))
            ->filter(ref('id')->equals(lit(2)))
            ->fetch();

        static::assertCount(0, $rows);
        static::assertCount(1, $log);
    }

    public function test_an_abandoned_get_each_leaves_no_consumed_step_for_the_next_run(): void
    {
        $dataFrame = df()
            ->read(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
                rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 4])),
            ))
            ->limit(3);

        // the reference keeps the generator parked, so its steps stay consumed
        $parked = $dataFrame->getEach();
        $parked->current();

        static::assertCount(3, $dataFrame->fetch());
    }

    public function test_an_error_handler_set_after_schema_reaches_a_sink_root(): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(new Branched(ref('id')->equals(lit(1)), new ThrowingLoader(new RuntimeException('boom'))));

        $dataFrame->schema();
        $dataFrame->onError($handler);
        $dataFrame->run();

        static::assertCount(1, $handler->errors);
    }

    public function test_on_error_sets_the_handler(): void
    {
        $context = flow_context();
        $handler = new IgnoreError();

        (new DataFrame(from_array([['id' => 1]]), $context))->onError($handler);

        static::assertSame($handler, $context->errorHandler());
    }

    public function test_on_error_on_a_fork_leaves_the_parents_handler_untouched(): void
    {
        $context = flow_context();
        $handler = $context->errorHandler();

        (new DataFrame(from_array([['id' => 1]]), $context))
            ->fork()
            ->onError(new IgnoreError());

        static::assertSame($handler, $context->errorHandler());
    }

    public function test_a_fork_keeps_the_parents_root_identity(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));

        static::assertSame($dataFrame->cursor(), $dataFrame->fork()->cursor());
    }

    public function test_schema_then_a_run_plans_twice(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $dataFrame = df(config_builder()->planner(new Planner(Lowerings::default(), new RecordingRule('plan', $log))))
            ->read(from_array([['id' => 1]]));

        $dataFrame->schema();
        $dataFrame->run();

        static::assertCount(2, $log);
    }

    public function test_a_fork_shares_the_root_and_builds_without_touching_the_frame(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $root = $dataFrame->cursor();

        $fork = $dataFrame->fork();
        $fork->select('id');

        static::assertNotSame($dataFrame, $fork);
        static::assertSame($root, $dataFrame->cursor());
        static::assertSame([$root], $fork->cursor()->children());
    }

    public function test_a_limit_inside_an_inlined_nested_frame_is_pushed_into_its_source(): void
    {
        $extractor = new RecordingScanExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        $inner = df()->read($extractor)->limit(1);

        static::assertSame([['id' => 1]], df()->read(from_data_frame($inner))->fetch()->toArray());
        static::assertSame(1, $extractor->scans[0]->limit);
    }

    public function test_logical_has_one_result_root_without_sinks(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]))->select('id');

        $root = $dataFrame->logical()->root;

        static::assertInstanceOf(Node\Result::class, $root);
        static::assertSame([$dataFrame->cursor()], $root->children());
        static::assertSame([], $dataFrame->logical()->sinkRoots());
    }

    public function test_logical_wraps_the_result_and_every_sink_in_a_sink_multiple(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(to_memory(new ArrayMemory()))
            ->select('id');

        $root = $dataFrame->logical()->root;

        static::assertInstanceOf(Node\SinkMultiple::class, $root);
        static::assertInstanceOf(Node\Result::class, $root->children()[0]);
        static::assertSame([$dataFrame->cursor()], $root->children()[0]->children());
        static::assertSame($dataFrame->sinks(), $dataFrame->logical()->sinkRoots());
        static::assertCount(1, $dataFrame->sinks());
    }

    public function test_explain_prints_the_logical_plan_without_reading_a_row(): void
    {
        $extractor = new RecordingScanExtractor(schema(int_schema('id')));
        $dataFrame = df()->read($extractor)->filter(ref('id')->isNotNull())->write(to_memory(new ArrayMemory()));

        static::assertSame((new Explain())->of($dataFrame->logical()), $dataFrame->explain());
        static::assertStringContainsString('#3 (shared)', $dataFrame->explain());
        static::assertSame([], $extractor->scans);
    }

    public function test_a_fork_carries_none_of_the_frames_sinks(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(to_memory(new ArrayMemory()))
            ->write(to_memory(new ArrayMemory()));

        static::assertCount(2, $dataFrame->sinks());
        static::assertSame([], $dataFrame->fork()->sinks());
        static::assertSame($dataFrame->cursor(), $dataFrame->fork()->cursor());
    }
}
