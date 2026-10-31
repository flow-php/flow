<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API;

use ArrayObject;
use DateTimeImmutable;
use Flow\ETL\API\DataFrame;
use Flow\ETL\API\FilteredBranch;
use Flow\ETL\API\Transformation;
use Flow\ETL\API\Transformations;
use Flow\ETL\API\TransformedBranch;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;
use Flow\ETL\Planning\Logical\Format;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Stage;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\AddStampToStringColumnTransformer;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\CallOrderLoader;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\RowLessExtractor;
use Flow\ETL\Tests\Double\SpyBranch;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\DataFrameMother;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Validator\SelectiveValidator;
use Generator;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\Attributes\TestWith;
use RuntimeException;
use SensitiveParameter;

use function array_column;
use function array_merge;
use function Flow\ETL\DSL\add_row_index;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\limit;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_transformation;
use function Flow\Types\DSL\type_json;
use function iterator_to_array;
use function ob_get_clean;
use function ob_start;

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
            ->write($spy)
            ->withColumn('element', ref('elements')->expand())
            ->batchSize(3)
            ->forEach(function (Rows $rows): void {
                $this->assertLessThanOrEqual(3, $rows->count());
            });

        static::assertSame([1, 1, 1, 1, 1], $spy->loadedRowCounts());
    }

    public function test_collect_references(): void
    {
        df()
            ->read(from_all(
                from_array([
                    ['id' => 1, 'name' => 'test', 'active' => false],
                    ['id' => 1, 'name' => 'test', 'active' => false],
                    ['id' => 1, 'name' => 'test', 'active' => false],
                    ['id' => 1, 'name' => 'test', 'active' => false],
                ]),
                from_array([
                    ['id' => 1, 'name' => 'test', 'active' => false, 'country' => 'US'],
                    ['id' => 1, 'name' => 'test', 'active' => false, 'group' => 'A'],
                ]),
            ))
            ->collectRefs($refs = refs())
            ->run();

        static::assertEquals(refs('id', 'name', 'active', 'country', 'group'), $refs);
    }

    public function test_count(): void
    {
        static::assertSame(
            5,
            df()
                ->read(from_array([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                    ['id' => 4],
                    ['id' => 5],
                ]))
                ->count(),
        );
    }

    public function test_count_of_a_frame_that_yields_no_rows_is_zero(): void
    {
        static::assertSame(
            0,
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->filter(lit(false))
                ->count(),
        );
    }

    public function test_count_counts_the_rows_left_after_a_filter(): void
    {
        static::assertSame(
            2,
            df()
                ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
                ->filter(ref('id')->greaterThan(lit(1)))
                ->count(),
        );
    }

    public function test_count_still_runs_the_frames_writes(): void
    {
        $memory = new ArrayMemory();

        static::assertSame(
            2,
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->write(to_memory($memory))
                ->count(),
        );
        static::assertSame([['id' => 1], ['id' => 2]], $memory->dump());
    }

    public function test_drop(): void
    {
        static::assertEquals(
            array_to_rows(
                [
                    ['name' => 'foo', 'active' => true],
                    ['name' => null, 'active' => false],
                    ['name' => 'bar', 'active' => false],
                ],
                schema(str_schema('name', nullable: true), bool_schema('active')),
            ),
            df()
                ->process(array_to_rows(
                    [
                        ['id' => 1, 'name' => 'foo', 'active' => true],
                        ['id' => 2, 'name' => null, 'active' => false],
                        ['id' => 2, 'name' => 'bar', 'active' => false],
                    ],
                    schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                ))
                ->drop('id')
                ->fetch(),
        );
    }

    public function test_drop_duplicates(): void
    {
        static::assertEquals(
            array_to_rows(
                [['id' => 1, 'name' => 'foo', 'active' => true], ['id' => 2, 'name' => 'bar', 'active' => false]],
                schema(int_schema('id'), str_schema('name'), bool_schema('active')),
            ),
            df()
                ->process(array_to_rows(
                    [
                        ['id' => 1, 'name' => 'foo', 'active' => true],
                        ['id' => 2, 'name' => 'bar', 'active' => false],
                        ['id' => 2, 'name' => 'bar', 'active' => false],
                    ],
                    schema(int_schema('id'), str_schema('name'), bool_schema('active')),
                ))
                ->dropDuplicates(ref('id'))
                ->fetch(),
        );
    }

    public function test_encapsulate_transformations(): void
    {
        static::assertEquals(
            array_to_rows(
                [
                    ['country' => 'pl', 'age' => 2.0],
                    ['country' => 'pl', 'age' => 2.0],
                    ['country' => 'pl', 'age' => 2.5],
                    ['country' => 'pl', 'age' => 3.0],
                    ['country' => 'us', 'age' => 4.0],
                    ['country' => 'us', 'age' => 4.0],
                    ['country' => 'us', 'age' => 4.5],
                    ['country' => 'us', 'age' => 5.0],
                ],
                schema(str_schema('country'), float_schema('age')),
            ),
            df()
                ->process(array_to_rows(
                    [
                        ['id' => 1, 'country' => 'PL', 'age' => 20, 'gender' => 'male'],
                        ['id' => 2, 'country' => 'PL', 'age' => 20, 'gender' => 'male'],
                        ['id' => 3, 'country' => 'PL', 'age' => 25, 'gender' => 'male'],
                        ['id' => 4, 'country' => 'PL', 'age' => 30, 'gender' => 'female'],
                        ['id' => 5, 'country' => 'US', 'age' => 40, 'gender' => 'female'],
                        ['id' => 6, 'country' => 'US', 'age' => 40, 'gender' => 'male'],
                        ['id' => 7, 'country' => 'US', 'age' => 45, 'gender' => 'female'],
                        ['id' => 9, 'country' => 'US', 'age' => 50, 'gender' => 'male'],
                    ],
                    schema(int_schema('id'), str_schema('country'), int_schema('age'), str_schema('gender')),
                ))
                ->with(new class implements Transformation {
                    public function transform(DataFrame $dataFrame): DataFrame
                    {
                        return $dataFrame->withColumn('country', ref('country')->lower())->withColumn('age', ref(
                            'age',
                        )->divide(lit(10), scale: 2));
                    }
                })
                ->with(new class implements Transformation {
                    public function transform(DataFrame $dataFrame): DataFrame
                    {
                        return $dataFrame->drop('gender')->drop('id');
                    }
                })
                ->fetch(),
        );
    }

    public function test_filter(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(integer_schema('id'));
                }

                /**
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(Backend $backend, Criteria $criteria): Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield array_to_rows([['id' => $i]], schema(integer_schema('id')));
                    }
                }

                public function statistics(): Statistics
                {
                    return new Statistics();
                }

                public function isRepeatable(): bool
                {
                    return false;
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
            ->process(array_to_rows(
                [
                    ['id' => 1, 'name' => 'foo', 'active' => true],
                    ['id' => 2, 'name' => null, 'active' => false],
                    ['id' => 2, 'name' => 'bar', 'active' => false],
                ],
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
            ))
            ->foreach(function (Rows $rows): void {
                $this->assertEquals(
                    array_to_rows(
                        [
                            ['id' => 1, 'name' => 'foo', 'active' => true],
                            ['id' => 2, 'name' => null, 'active' => false],
                            ['id' => 2, 'name' => 'bar', 'active' => false],
                        ],
                        schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                    ),
                    $rows,
                );
            });
    }

    public function test_get(): void
    {
        $rows = df()->read(from_rows(
            $extractedRows = array_to_rows(
                [
                    ['id' => 1, 'name' => 'foo'],
                    ['id' => 2, 'name' => 'bar'],
                    ['id' => 3, 'name' => 'baz'],
                    ['id' => 4, 'name' => 'foo'],
                    ['id' => 5, 'name' => 'bar'],
                    ['id' => 6, 'name' => 'baz'],
                ],
                schema(int_schema('id'), str_schema('name')),
            ),
        ))->get();

        static::assertEquals([$extractedRows], iterator_to_array($rows));
    }

    public function test_get_as_array(): void
    {
        $rows = df()
            ->read(from_rows(
                $extractedRows = array_to_rows(
                    [
                        ['id' => 1, 'name' => 'foo'],
                        ['id' => 2, 'name' => 'bar'],
                        ['id' => 3, 'name' => 'baz'],
                        ['id' => 4, 'name' => 'foo'],
                        ['id' => 5, 'name' => 'bar'],
                        ['id' => 6, 'name' => 'baz'],
                    ],
                    schema(int_schema('id'), str_schema('name')),
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

    public function test_get_each_as_array(): void
    {
        static::assertEquals(
            [
                ['id' => 1, 'name' => 'foo'],
                ['id' => 2, 'name' => 'bar'],
                ['id' => 3, 'name' => 'baz'],
                ['id' => 4, 'name' => 'foo'],
                ['id' => 5, 'name' => 'bar'],
                ['id' => 6, 'name' => 'baz'],
            ],
            iterator_to_array(
                df()
                    ->read(from_rows(array_to_rows(
                        [
                            ['id' => 1, 'name' => 'foo'],
                            ['id' => 2, 'name' => 'bar'],
                            ['id' => 3, 'name' => 'baz'],
                            ['id' => 4, 'name' => 'foo'],
                            ['id' => 5, 'name' => 'bar'],
                            ['id' => 6, 'name' => 'baz'],
                        ],
                        schema(int_schema('id'), str_schema('name')),
                    )))
                    ->getEachAsArray(),
            ),
        );
    }

    public function test_deriving_a_boolean_entry_from_an_existing_column(): void
    {
        $rows = data_frame()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(integer_schema('id'));
                }

                /**
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(Backend $backend, Criteria $criteria): Generator
                {
                    for ($i = 1; $i <= 10; $i++) {
                        yield array_to_rows([['id' => $i]], schema(integer_schema('id')));
                    }
                }

                public function statistics(): Statistics
                {
                    return new Statistics();
                }

                public function isRepeatable(): bool
                {
                    return false;
                }
            })
            ->withColumn('odd', ref('id')->mod(lit(2))->equals(lit(0)))
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
             * @return \Generator<int, Rows, Signal|null, void>
             */
            public function extract(Backend $backend, Criteria $criteria): Generator
            {
                yield array_to_rows(
                    [[
                        'id' => 101,
                        'deleted' => false,
                        'expiration-date' => new DateTimeImmutable('2020-08-24'),
                        'phase' => null,
                    ]],
                    schema(
                        int_schema('id'),
                        bool_schema('deleted'),
                        datetime_schema('expiration-date'),
                        str_schema('phase', nullable: true),
                    ),
                );

                yield array_to_rows(
                    [[
                        'id' => 102,
                        'deleted' => true,
                        'expiration-date' => new DateTimeImmutable('2020-08-25'),
                        'phase' => null,
                    ]],
                    schema(
                        int_schema('id'),
                        bool_schema('deleted'),
                        datetime_schema('expiration-date'),
                        str_schema('phase', nullable: true),
                    ),
                );
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }

            public function isRepeatable(): bool
            {
                return false;
            }
        };

        $addStampStringEntry = new class implements Transformer {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
            }

            public function describe(): ?Description
            {
                return null;
            }

            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input->add(str_schema('stamp')));
            }

            public function transform(Rows $rows, Backend $backend): Rows
            {
                $stamped = [];

                for ($i = 0; $i < $rows->count(); $i++) {
                    $stamped[] = [...$rows->values($i), 'stamp' => 'zero'];
                }

                return array_to_rows($stamped, $rows->schema()->add(str_schema('stamp')));
            }
        };

        $loader = new class implements Loader {
            public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
            {
                throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
            }

            public function describe(): ?Description
            {
                return null;
            }

            /** @var array<array-key, mixed> */
            public array $result = [];

            public function load(Rows $rows, Backend $backend): void
            {
                $this->result = array_merge($this->result, $rows->toArray());
            }
        };

        data_frame()
            ->read($extractor)
            ->onError(new IgnoreError())
            ->with($addStampStringEntry)
            ->with(new class implements Transformer {
                public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input);
                }

                public function transform(Rows $rows, Backend $backend): Rows
                {
                    throw new RuntimeException('Unexpected exception');
                }
            })
            ->with(AddStampToStringColumnTransformer::divideBySemicolon('stamp', 'one'))
            ->with(AddStampToStringColumnTransformer::divideBySemicolon('stamp', 'two'))
            ->with(AddStampToStringColumnTransformer::divideBySemicolon('stamp', 'three'))
            ->write($loader)
            ->run();

        // IgnoreError drops a batch whose transformation failed, so the stamps after it never run
        static::assertSame([], $loader->result);
    }

    public function test_process_constructor(): void
    {
        $collectedRows = data_frame()
            ->process(
                $rows = array_to_rows(
                    [[
                        'id' => 101,
                        'deleted' => false,
                        'expiration-date' => new DateTimeImmutable('2020-08-24'),
                        'phase' => null,
                    ]],
                    schema(
                        int_schema('id'),
                        bool_schema('deleted'),
                        datetime_schema('expiration-date'),
                        str_schema('phase', nullable: true),
                    ),
                ),
            )
            ->fetch();

        static::assertEquals($rows, $collectedRows);
    }

    public function test_select(): void
    {
        static::assertEquals(
            array_to_rows(
                [['name' => 'foo', 'id' => 1], ['name' => null, 'id' => 2], ['name' => 'bar', 'id' => 2]],
                schema(str_schema('name', nullable: true), int_schema('id')),
            ),
            data_frame()
                ->process(array_to_rows(
                    [
                        ['id' => 1, 'name' => 'foo', 'active' => true],
                        ['id' => 2, 'name' => null, 'active' => false],
                        ['id' => 2, 'name' => 'bar', 'active' => false],
                    ],
                    schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
                ))
                ->select('name', 'id')
                ->fetch(),
        );
    }

    public function test_selective_validation_against_schema(): void
    {
        $rows = data_frame()
            ->process(array_to_rows(
                [
                    ['id' => 1, 'name' => 'foo', 'active' => true],
                    ['id' => 2, 'name' => null, 'tags' => type_json()->cast(['foo', 'bar'])],
                    ['id' => 2, 'name' => 'bar', 'active' => false],
                ],
                schema(
                    int_schema('id'),
                    str_schema('name', nullable: true),
                    bool_schema('active', nullable: true),
                    json_schema('tags', nullable: true),
                ),
            ))
            ->match(schema(integer_schema('id', false)), new SelectiveValidator())
            ->fetch();

        static::assertSame(
            array_to_rows(
                [
                    ['id' => 1, 'name' => 'foo', 'active' => true],
                    ['id' => 2, 'name' => null, 'tags' => type_json()->cast(['foo', 'bar'])],
                    ['id' => 2, 'name' => 'bar', 'active' => false],
                ],
                schema(
                    int_schema('id'),
                    str_schema('name', nullable: true),
                    bool_schema('active', nullable: true),
                    json_schema('tags', nullable: true),
                ),
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
            ->process(array_to_rows(
                [
                    ['id' => 1, 'name' => 'foo', 'active' => true],
                    ['id' => 2, 'name' => null, 'active' => false],
                    ['id' => 2, 'name' => 'bar', 'active' => false],
                ],
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
            ))
            ->match(schema(integer_schema('id', false), string_schema('name', true), bool_schema('active', false)))
            ->fetch();

        static::assertSame(
            array_to_rows(
                [
                    ['id' => 1, 'name' => 'foo', 'active' => true],
                    ['id' => 2, 'name' => null, 'active' => false],
                    ['id' => 2, 'name' => 'bar', 'active' => false],
                ],
                schema(int_schema('id'), str_schema('name', nullable: true), bool_schema('active')),
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
        // void() drops rows, not columns - the batch it yields carries the plan's declared schema, and
        // the global aggregate that follows it reads zero rows, so it emits its initial accumulators
        static::assertEquals(
            array_to_rows([['average_age' => null]], schema(float_schema('average_age', nullable: true))),
            data_frame()
                ->process(array_to_rows(
                    [
                        ['id' => 1, 'country' => 'PL', 'age' => 20],
                        ['id' => 2, 'country' => 'PL', 'age' => 20],
                        ['id' => 3, 'country' => 'PL', 'age' => 25],
                        ['id' => 4, 'country' => 'PL', 'age' => 30],
                        ['id' => 5, 'country' => 'US', 'age' => 40],
                        ['id' => 6, 'country' => 'US', 'age' => 40],
                        ['id' => 7, 'country' => 'US', 'age' => 45],
                        ['id' => 9, 'country' => 'US', 'age' => 50],
                    ],
                    schema(int_schema('id'), str_schema('country'), int_schema('age')),
                ))
                ->rename('country', 'country_code')
                ->void()
                ->aggregate([average(ref('age'))])
                ->rename('age_avg', 'average_age')
                ->fetch(),
        );
    }

    public function test_with_batch_size(): void
    {
        data_frame()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(integer_schema('id'));
                }

                /**
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(Backend $backend, Criteria $criteria): Generator
                {
                    yield array_to_rows([
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
                    ], schema(integer_schema('id')));
                }

                public function statistics(): Statistics
                {
                    return new Statistics();
                }

                public function isRepeatable(): bool
                {
                    return false;
                }
            })
            ->with(new class implements Transformer {
                public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input->rename('id', 'new_id'));
                }

                public function transform(Rows $rows, Backend $backend): Rows
                {
                    return $rows->withSchema($rows->schema()->rename('id', 'new_id'));
                }
            })
            ->batchSize(2)
            ->write(new class implements Loader {
                public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function load(Rows $rows, Backend $backend): void
                {
                    Assert::assertCount(2, $rows);
                }
            })
            ->run();
    }

    public function test_with_collecting(): void
    {
        data_frame()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(integer_schema('id'));
                }

                /**
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(Backend $backend, Criteria $criteria): Generator
                {
                    yield array_to_rows([['id' => 1]], schema(integer_schema('id')));
                    yield array_to_rows([['id' => 2]], schema(integer_schema('id')));
                    yield array_to_rows([['id' => 3]], schema(integer_schema('id')));
                }

                public function statistics(): Statistics
                {
                    return new Statistics();
                }

                public function isRepeatable(): bool
                {
                    return false;
                }
            })
            ->with(new class implements Transformer {
                public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input->rename('id', 'new_id'));
                }

                public function transform(Rows $rows, Backend $backend): Rows
                {
                    return $rows->withSchema($rows->schema()->rename('id', 'new_id'));
                }
            })
            ->collect()
            ->write(new class implements Loader {
                public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function load(Rows $rows, Backend $backend): void
                {
                    Assert::assertCount(3, $rows);
                }
            })
            ->run();
    }

    public function test_fetch_carries_the_source_schema_when_the_source_yields_no_rows(): void
    {
        $rows = data_frame()
            ->read(new RowLessExtractor(schema(int_schema('id', true), str_schema('name', true))))
            ->fetch();

        static::assertCount(0, $rows);
        static::assertTrue($rows->schema()->isSame(schema(int_schema('id', true), str_schema('name', true))));
    }

    public function test_fetch_does_not_run_the_pipeline_twice_when_the_source_yields_no_rows(): void
    {
        $extractor = new RowLessExtractor(schema(int_schema('id', true)));

        data_frame()->read($extractor)->fetch();

        static::assertSame(1, $extractor->extractCalls);
    }

    public function test_fetch_is_empty_when_a_row_less_source_cannot_describe_itself(): void
    {
        $rows = data_frame()->read(new UndescribableRowLessExtractor())->fetch();

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
            ->read(new RowLessExtractor(schema(int_schema('id', true))))
            ->drop('id')
            ->fetch();

        static::assertCount(0, $rows);
        static::assertCount(0, $rows->schema()->definitions());
    }

    public function test_a_pushed_limit_reaches_extract_and_leaves_the_users_extractor_alone(): void
    {
        $extractor = new RecordingFileExtractor(
            schema(int_schema('id')),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        $frame = df()
            ->read($extractor)
            ->withColumn('doubled', ref('id')->multiply(lit(2)))
            ->limit(1);

        $frame->schema();

        static::assertSame([['id' => 1, 'doubled' => 2]], $frame->fetch()->toArray());
        static::assertSame([['id' => 1, 'doubled' => 2]], $frame->fetch()->toArray());
        static::assertCount(2, $extractor->criteria);
        static::assertSame(1, $extractor->criteria[0]->limit);
        static::assertSame(1, $extractor->criteria[1]->limit);
        static::assertStringEndsWith(
            "#1 Read\n            Extractor: RecordingFileExtractor\n            Source: file:///dev/null",
            $frame->explain()->toString(Stage::unoptimized),
        );
        static::assertStringEndsWith(
            "#1 Read\n            Extractor: RecordingFileExtractor\n            Source: file:///dev/null"
            . "\n            Limit: 1",
            $frame->explain()->toString(),
        );
    }

    public function test_a_limit_inside_a_joins_right_side_is_pushed_into_that_source(): void
    {
        $extractor = new RecordingFileExtractor(
            schema(int_schema('id')),
            array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
        );
        $right = df()->read($extractor)->limit(1);

        df()
            ->read(from_array([['id' => 1]]))
            ->join($right, join_on(['id' => 'id'], 'r_'))
            ->fetch();

        static::assertSame(1, $extractor->criteria[0]->limit);
    }

    public function test_an_empty_fetch_plans_once(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();

        static::assertCount(
            0,
            df(config_builder()->optimizer(new Optimizer(new RecordingRule('plan', $log))))
                ->read(from_array([['id' => 1]]))
                ->filter(ref('id')->equals(lit(2)))
                ->fetch(),
        );
        static::assertCount(1, $log);
    }

    public function test_an_abandoned_get_each_as_array_leaves_no_consumed_step_for_the_next_run(): void
    {
        $dataFrame = df()
            ->read(from_rows(
                array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
                array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id'))),
            ))
            ->limit(3);

        // the reference keeps the generator parked, so its steps stay consumed
        $parked = $dataFrame->getEachAsArray();
        $parked->current();

        static::assertCount(3, $dataFrame->fetch());
    }

    public function test_an_error_handler_set_after_schema_reaches_a_branch(): void
    {
        $handler = new RecordingErrorHandler(new IgnoreError());
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(new FilteredBranch(ref('id')->equals(lit(1)), new ThrowingLoader(new RuntimeException('boom'))));

        $dataFrame->schema();
        $dataFrame = $dataFrame->onError($handler);
        $dataFrame->run();

        static::assertCount(1, $handler->errors);
    }

    public function test_on_error_leaves_the_flow_context_the_frame_was_built_on_unchanged(): void
    {
        $context = flow_context();

        $guarded = DataFrameMother::over(from_array([['id' => 1]]), $context)->onError(new IgnoreError());
        $guarded->run();

        static::assertInstanceOf(ThrowError::class, $context->errorHandler());
    }

    public function test_on_error_on_one_frame_leaves_a_frame_over_the_same_flow_context_unchanged(): void
    {
        $context = flow_context();
        $guarded = DataFrameMother::over(from_array([['id' => 1]]), $context);
        $other = DataFrameMother::over(from_array([['id' => 1]]), $context);

        $guarded
            ->onError(new IgnoreError())
            ->with(new ThrowingTransformer(new RuntimeException('boom')))
            ->run();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('boom');

        $other->with(new ThrowingTransformer(new RuntimeException('boom')))->run();
    }

    public function test_on_error_on_one_frame_leaves_a_frame_built_from_the_same_config_unchanged(): void
    {
        $config = config();
        $guarded = df($config)->read(from_array([['id' => 1]]));
        $other = df($config)->read(from_array([['id' => 1]]));

        $guarded
            ->onError(new IgnoreError())
            ->with(new ThrowingTransformer(new RuntimeException('boom')))
            ->run();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('boom');

        $other->with(new ThrowingTransformer(new RuntimeException('boom')))->run();
    }

    public function test_a_branchs_steps_run_under_the_outer_frames_handler(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->onError(new IgnoreError())
            ->write(new TransformedBranch(
                new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => $prefix->with(new ThrowingTransformer(
                    new RuntimeException('boom'),
                ))),
                to_memory($memory),
            ))
            ->run();

        static::assertSame([], $memory->dump());
    }

    public function test_a_consumer_closure_failure_inside_a_branch_propagates_without_an_offer(): void
    {
        $handler = new RecordingErrorHandler(new ThrowError());
        $thrown = null;

        try {
            df()
                ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
                ->batchSize(1)
                ->onError($handler)
                ->write(to_transformation(limit(1), new ClosureThrowingLoader(new RuntimeException('closure boom'))))
                ->run();
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertSame('closure boom', $thrown?->getMessage());
        static::assertSame([], $handler->errors);
    }

    public function test_a_processor_failure_inside_a_branch_propagates_without_an_offer(): void
    {
        $handler = new RecordingErrorHandler(new ThrowError());
        $thrown = null;

        try {
            df()
                ->read(from_array([['id' => 2], ['id' => 2]]))
                ->onError($handler)
                ->write(to_transformation(
                    new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => $prefix->dropDuplicates(ref(
                        'nope',
                    ))),
                    to_memory(new ArrayMemory()),
                ))
                ->run();
        } catch (InvalidArgumentException $e) {
            $thrown = $e;
        }

        static::assertSame('Column "nope" does not exist.', $thrown?->getMessage());
        static::assertSame([], $handler->errors);
    }

    public function test_a_loader_failure_inside_a_branch_is_offered_once_against_the_users_loader(): void
    {
        $handler = new RecordingErrorHandler(new ThrowError());
        $loader = new ThrowingLoader(new RuntimeException('load boom'));

        try {
            df()
                ->read(from_array([['id' => 1]]))
                ->onError($handler)
                ->write(to_transformation(limit(5), $loader))
                ->run();
            static::fail('expected the loader failure to propagate');
        } catch (RuntimeException) {
        }

        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(LoadingError::class, $handler->errors[0]);
        static::assertSame($loader, $handler->errors[0]->loader);
    }

    public function test_a_branch_without_on_error_writes_its_rows(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(new TransformedBranch(
                new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => $prefix->withColumn(
                    'twice',
                    ref('id')->multiply(lit(2)),
                )),
                to_memory($memory),
            ))
            ->run();

        static::assertSame([['id' => 1, 'twice' => 2], ['id' => 2, 'twice' => 4]], $memory->dump());
    }

    public function test_on_error_inside_a_branchs_transformation_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A branch must return a frame built from the prefix it was given: '
            . TransformedBranch::class
            . ' returned another frame. Build on $prefix, and call onError() on the frame you write to, not inside '
            . 'the branch',
        );

        df()
            ->read(from_array([['id' => 1]]))
            ->write(new TransformedBranch(
                new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => $prefix->onError(
                    new IgnoreError(),
                )),
                to_memory(new ArrayMemory()),
            ))
            ->run();
    }

    public function test_a_branch_returning_a_frame_not_built_from_its_prefix_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A branch must return a frame built from the prefix it was given: '
            . TransformedBranch::class
            . ' returned another frame. Build on $prefix, and call onError() on the frame you write to, not inside '
            . 'the branch',
        );

        df()
            ->read(from_array([['id' => 1]]))
            ->write(new TransformedBranch(
                new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => df()->read(from_array([
                    ['x' => 1],
                ]))),
                to_memory(new ArrayMemory()),
            ))
            ->run();
    }

    public function test_a_branch_without_on_error_is_not_refused(): void
    {
        $memory = new ArrayMemory();

        df()
            ->read(from_array([['id' => 1]]))
            ->write(new TransformedBranch(select('id'), to_memory($memory)))
            ->run();

        static::assertSame([['id' => 1]], $memory->dump());
    }

    public function test_a_branch_that_cannot_describe_its_rows_does_not_hide_the_frames_schema(): void
    {
        static::assertEquals(
            schema(int_schema('id'), int_schema('double')),
            df()
                ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
                ->withColumn('double', ref('id')->multiply(lit(2)))
                ->write(to_transformation(
                    new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->joinEach(
                        new StaticExtractorFactory(from_array([['id' => 1, 'name' => 'a']])),
                        join_on(['id' => 'id'], 'joined_'),
                    )),
                    to_memory(new ArrayMemory()),
                ))
                ->schema(),
        );
    }

    public function test_a_row_index_starts_again_on_every_run(): void
    {
        $frame = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))->with(add_row_index('idx'));

        static::assertSame([0, 1, 2], array_column($frame->fetch()->toArray(), 'idx'));
        static::assertSame([0, 1, 2], array_column($frame->fetch()->toArray(), 'idx'));
    }

    public function test_schema_then_a_run_plans_twice(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $dataFrame = df(config_builder()->optimizer(new Optimizer(new RecordingRule('plan', $log))))
            ->read(from_array([['id' => 1]]));

        $dataFrame->schema();
        $dataFrame->run();

        static::assertCount(2, $log);
    }

    public function test_explain_without_writes_prints_one_result_over_the_chain(): void
    {
        static::assertSame(
            <<<'PLAN'
                #3 Result  preserving · transparent
                │  Rows this plan hands out: to the trigger, or to the node reading it
                └─ #2 Select  preserving · transparent · redefines all but id
                   │  Keeps only: id
                   └─ #1 Read  source · transparent
                         Extractor: ArrayExtractor
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->select('id')
                ->explain()
                ->toString(format: Format::declarations),
        );
    }

    public function test_explain_with_a_branch_prints_the_outputs_sharing_the_chain(): void
    {
        static::assertSame(
            <<<'PLAN'
                Outputs  preserving · opaque
                ├─ #3 Result  preserving · transparent
                │  │  Rows this plan hands out: to the trigger, or to the node reading it
                │  └─ #2 Select  preserving · transparent · redefines all but id
                │     │  Keeps only: id
                │     └─ #1 Read  source · transparent
                │           Extractor: ArrayExtractor
                └─ #4 Write  preserving · opaque
                   │  Loader: MemoryLoader
                   └─ #1 Read (shared)
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(to_memory(new ArrayMemory()))
                ->select('id')
                ->explain()
                ->toString(format: Format::declarations),
        );
    }

    public function test_explain_of_the_run_trigger_makes_the_write_the_root(): void
    {
        static::assertSame(
            <<<'PLAN'
                #2 Write  preserving · opaque
                │  Loader: MemoryLoader
                └─ #1 Read  source · transparent
                      Extractor: ArrayExtractor
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(to_memory(new ArrayMemory()))
                ->explain(Trigger::run)
                ->toString(format: Format::declarations),
        );
    }

    public function test_explain_of_the_run_trigger_keeps_a_result_over_a_chain_end_no_write_reads(): void
    {
        static::assertSame(
            <<<'PLAN'
                Outputs  preserving · opaque
                ├─ #3 Result  preserving · transparent
                │  │  Rows this plan hands out: to the trigger, or to the node reading it
                │  └─ #2 Select  preserving · transparent · redefines all but id
                │     │  Keeps only: id
                │     └─ #1 Read  source · transparent
                │           Extractor: ArrayExtractor
                └─ #4 Write  preserving · opaque
                   │  Loader: MemoryLoader
                   └─ #1 Read (shared)
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(to_memory(new ArrayMemory()))
                ->select('id')
                ->explain(Trigger::run)
                ->toString(format: Format::declarations),
        );
    }

    public function test_for_each_receives_every_batch(): void
    {
        $batches = [];

        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->batchSize(2)
            ->forEach(static function (Rows $rows) use (&$batches): void {
                $batches[] = $rows->count();
            });

        static::assertSame([2, 1], $batches);
    }

    public function test_explain_prints_the_logical_plan_without_reading_a_row(): void
    {
        $extractor = (new RecordingFileExtractor(schema(int_schema('id'))))->withPruning();

        static::assertSame(<<<'PLAN'
            #2 Write  preserving · opaque
            │  Loader: MemoryLoader
            └─ #1 Read  source · transparent
                  Extractor: RecordingFileExtractor
                  Source: file:///dev/null
                  Filter: id IS NOT NULL
            PLAN, df()
            ->read($extractor)
            ->filter(ref('id')->isNotNull())
            ->write(to_memory(new ArrayMemory()))
            ->explain()
            ->toString(format: Format::declarations));
        static::assertSame([], $extractor->criteria);
    }

    /**
     * @param int<-1, 0> $size
     */
    #[TestWith([-1])]
    #[TestWith([0])]
    public function test_batch_size_below_one_collects_every_row_into_one_batch(int $size): void
    {
        $batches = iterator_to_array(
            df()
                ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
                ->batchSize(1)
                ->batchSize($size)
                ->get(),
            false,
        );

        static::assertCount(1, $batches);
        static::assertSame(3, $batches[0]->count());
    }

    public function test_cache_refuses_a_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cache batch size must be greater than 0');

        df()
            ->read(from_array([['id' => 1]]))
            ->cache(cacheBatchSize: 0)
            ->fetch();
    }

    public function test_join_takes_the_join_type_as_a_string(): void
    {
        static::assertSame(
            [['id' => 1, 'r_id' => 1, 'r_x' => 'a']],
            df()
                ->read(from_array([['id' => 1], ['id' => 2]]))
                ->join(df()->read(from_array([['id' => 1, 'x' => 'a']])), join_on(['id' => 'id'], 'r_'), 'inner')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_join_each_refuses_an_unsupported_join_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported join type');

        df()
            ->read(from_array([['id' => 1]]))
            ->joinEach(new StaticExtractorFactory(from_array([['id' => 1]])), join_on(['id' => 'id']), 'nope')
            ->fetch();
    }

    public function test_with_applies_every_transformation_of_a_transformations_group(): void
    {
        static::assertSame(
            [['id' => 1]],
            df()
                ->read(from_array([['id' => 1, 'name' => 'a']]))
                ->with(new Transformations(select('id')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_an_error_handler_set_after_join_does_not_reach_the_joined_frame(): void
    {
        $left = df()->read(from_array([['id' => 1], ['id' => 2]]));
        $right = df()
            ->read(from_array([['id' => 1, 'x' => 'a']]))
            ->with(new ThrowingTransformer(new RuntimeException('right boom')));
        $left = $left->join($right, join_on(['id' => 'id'], 'r_'));
        $right->onError(new IgnoreError())->fetch();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('right boom');

        $left->fetch();
    }

    public function test_an_error_handler_set_after_cross_join_does_not_reach_the_joined_frame(): void
    {
        $left = df()->read(from_array([['id' => 1], ['id' => 2]]));
        $right = df()
            ->read(from_array([['x' => 'a']]))
            ->with(new ThrowingTransformer(new RuntimeException('right boom')));
        $left = $left->crossJoin($right, 'r_');
        $right->onError(new IgnoreError())->fetch();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('right boom');

        $left->fetch();
    }

    public function test_a_branch_is_handed_a_frame_without_the_callers_writes(): void
    {
        $spy = new SpyBranch();

        $frame = df()
            ->read(from_array([['id' => 1]]))
            ->write(to_memory(new ArrayMemory()))
            ->write(to_memory(new ArrayMemory()))
            ->write($spy);

        static::assertInstanceOf(DataFrame::class, $spy->prefix);
        static::assertStringStartsWith('#2 Result', $spy->prefix->explain()->toString());
        static::assertStringStartsWith('Outputs', $frame->explain()->toString());
    }

    public function test_a_branch_is_handed_a_frame_other_than_the_caller(): void
    {
        $frame = df()->read(from_array([['id' => 1]]));
        $spy = new SpyBranch();

        $written = $frame->write($spy);

        static::assertInstanceOf(DataFrame::class, $spy->prefix);
        static::assertNotSame($frame, $spy->prefix);
        static::assertNotSame($written, $spy->prefix);
    }

    public function test_two_branches_share_the_chain_and_do_not_alias(): void
    {
        static::assertSame(
            <<<'PLAN'
                Outputs  preserving · opaque
                ├─ #2 Result  preserving · transparent
                │  │  Rows this plan hands out: to the trigger, or to the node reading it
                │  └─ #1 Read  source · transparent
                │        Extractor: ArrayExtractor
                ├─ #4 Write  preserving · opaque
                │  │  Loader: MemoryLoader
                │  └─ #3 Transform  unknown · opaque · redefines unknown
                │     └─ #1 Read (shared)
                └─ #6 Write  preserving · opaque
                   │  Loader: MemoryLoader
                   └─ #5 Filter  reducing · transparent
                      │  Condition: id = 1
                      └─ #1 Read (shared)
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(
                    new TransformedBranch(
                        new AddRowIndexTransformer('idx', StartFrom::ZERO),
                        to_memory(new ArrayMemory()),
                    ),
                )
                ->write(new FilteredBranch(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())))
                ->explain()
                ->toString(format: Format::declarations),
        );
    }

    public function test_a_write_inside_a_branchs_transformation_runs_before_the_branchs_own_write(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();

        df()
            ->read(from_sequence_number('id', 0, 3))
            ->batchSize(2)
            ->write(
                new TransformedBranch(
                    new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => $prefix->write(new CallOrderLoader(
                        'inner',
                        $log,
                    ))),
                    new CallOrderLoader('outer', $log),
                ),
            )
            ->run();

        static::assertSame(['inner:2', 'outer:2', 'inner:2', 'outer:2'], $log->getArrayCopy());
    }

    public function test_a_joined_frame_is_part_of_the_outer_plan(): void
    {
        $right = df()->read(from_array([['id' => 1]]));

        $spine = df()
            ->read(from_array([['id' => 1]]))
            ->crossJoin($right, 'r_')
            ->explain()
            ->logical->spine();

        static::assertInstanceOf(CrossJoin::class, $spine);
        static::assertEquals($right->explain()->logical->root, $spine->children()[1]);
    }

    public function test_the_same_frame_cross_joined_twice_is_read_by_both_joins(): void
    {
        $right = df()->read(from_array([['n' => 1]]));

        static::assertSame(
            [['id' => 1, 'a_n' => 1, 'b_n' => 1]],
            df()
                ->read(from_array([['id' => 1]]))
                ->crossJoin($right, 'a_')
                ->crossJoin($right, 'b_')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_for_each_without_a_callback_still_executes_the_plan(): void
    {
        $loader = new SpyLoader();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->batchSize(1)
            ->write($loader)
            ->forEach();

        static::assertSame([1, 1], $loader->loadedRowCounts());
    }

    public function test_a_verb_on_the_joined_frame_after_the_join_does_not_reach_the_outer_plan(): void
    {
        $right = df()->read(from_array([['id' => 1, 'n' => 'a']]));
        $outer = df()->read(from_array([['id' => 1]]))->join($right, join_on(['id' => 'id'], 'r_'));

        static::assertSame(
            [],
            $right
                ->filter(ref('id')->equals(lit(2)))
                ->fetch()
                ->toArray(),
        );

        static::assertSame([['id' => 1, 'r_id' => 1, 'r_n' => 'a']], $outer->fetch()->toArray());
    }

    public function test_a_frame_reused_for_a_grouped_query_keeps_its_own_rows(): void
    {
        $df = df()
            ->read(from_array([
                ['date' => '2023-01-01', 'user' => 'user_01', 'commits' => 1, 'active' => false],
                ['date' => '2023-01-01', 'user' => 'user_02', 'commits' => 2, 'active' => true],
                ['date' => '2023-01-01', 'user' => 'user_03', 'commits' => 3, 'active' => true],
                ['date' => '2023-01-02', 'user' => 'user_01', 'commits' => 4, 'active' => true],
                ['date' => '2023-01-02', 'user' => 'user_02', 'commits' => 5, 'active' => true],
                ['date' => '2023-01-02', 'user' => 'user_03', 'commits' => 6, 'active' => true],
            ]))
            ->filter(ref('active')->isTrue());

        static::assertSame(
            [
                ['date' => '2023-01-01', 'commits_sum' => 5.0],
                ['date' => '2023-01-02', 'commits_sum' => 15.0],
            ],
            $df
                ->groupBy(ref('date'))
                ->aggregate(sum(ref('commits')))
                ->fetch()
                ->toArray(),
        );
        static::assertSame(
            [
                ['date' => '2023-01-01', 'user' => 'user_02', 'commits' => 2, 'active' => true],
                ['date' => '2023-01-01', 'user' => 'user_03', 'commits' => 3, 'active' => true],
                ['date' => '2023-01-02', 'user' => 'user_01', 'commits' => 4, 'active' => true],
                ['date' => '2023-01-02', 'user' => 'user_02', 'commits' => 5, 'active' => true],
                ['date' => '2023-01-02', 'user' => 'user_03', 'commits' => 6, 'active' => true],
            ],
            $df->fetch()->toArray(),
        );
    }

    public function test_select_returns_another_frame_and_leaves_the_original_columns(): void
    {
        $df = df()->read(from_array([['id' => 1, 'name' => 'a']]));

        static::assertNotSame($df, $df->select('id'));
        static::assertSame([['id' => 1, 'name' => 'a']], $df->fetch()->toArray());
    }

    public function test_fetch_with_a_limit_leaves_the_frame_unlimited(): void
    {
        $df = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]));

        static::assertCount(2, $df->fetch(2));
        static::assertCount(3, $df->fetch());
    }

    public function test_display_leaves_the_frame_unlimited(): void
    {
        $df = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]));

        $df->display(1);

        static::assertCount(3, $df->fetch());
    }

    public function test_print_rows_leaves_no_output_write_on_the_frame(): void
    {
        $df = df()->read(from_array([['id' => 1], ['id' => 2]]));

        ob_start();
        $df->printRows(1);
        ob_get_clean();
        ob_start();
        $df->run();

        static::assertSame('', ob_get_clean());
    }

    public function test_on_error_returns_another_frame_and_the_original_keeps_its_handler(): void
    {
        $df = df()->read(from_array([['id' => 1]]))->with(new ThrowingTransformer(new RuntimeException('boom')));
        $guarded = $df->onError(new IgnoreError());

        static::assertNotSame($df, $guarded);
        $guarded->run();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('boom');

        $df->run();
    }

    public function test_the_same_join_built_twice_on_one_frame_gives_the_same_rows(): void
    {
        $extractor = from_array([['id' => 1, 'g' => 0], ['id' => 2, 'g' => 1], ['id' => 4, 'g' => 0]]);
        $even = df()->read($extractor)->filter(ref('g')->equals(lit(0)));
        $all = df()->read($extractor)->select('id');

        static::assertCount(2, $even->join($all, join_on(['id' => 'id'], 'r_'))->fetch());
        static::assertCount(2, $even->join($all, join_on(['id' => 'id'], 'r_'))->fetch());
    }
}
