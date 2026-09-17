<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer;
use Flow\ETL\Planner;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_column;
use function array_map;
use function array_slice;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function range;

final class LimitTest extends FlowIntegrationTestCase
{
    public function test_exceeding_the_limit_in_one_rows_set(): void
    {
        $rows = df()
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 1000))))
            ->limit(9)
            ->fetch();

        static::assertCount(9, $rows);
    }

    public function test_fetch_with_limit(): void
    {
        $rows = df()
            ->from(from_array([
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

        static::assertCount(5, $rows);
    }

    public function test_fetch_with_limit_below_0(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        df()->read(from_rows(rows(schema())))->fetch(-1);
    }

    public function test_fetch_without_limit(): void
    {
        $rows = df()->read(new class implements Extractor {
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
            public function extract(FlowContext $context, ?int $limit = null): Generator
            {
                for ($i = 0; $i < 20; $i++) {
                    yield rows(schema(integer_schema('id')), row(['id' => $i]));
                }
            }
        })->fetch();

        static::assertCount(20, $rows);
    }

    public function test_limit(): void
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
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(FlowContext $context, ?int $limit = null): Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i + 1]), row(['id' => $i + 2]));
                    }
                }
            })
            ->limit(10)
            ->fetch();

        static::assertCount(10, $rows);
    }

    public function test_limit_below_0(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        df()->read(from_rows(rows(schema())))->limit(-1);
    }

    public function test_limit_null(): void
    {
        $rows = df()
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 10))))
            ->limit(null)
            ->fetch();

        static::assertCount(10, $rows);
    }

    public function test_limit_when_transformation_is_expanding_rows_extracted_from_extractor(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                public function withSchema(Schema $schema): static
                {
                    return $this;
                }

                public function schema(): Schema
                {
                    return schema(list_schema(
                        'ids',
                        type_list(type_structure([
                            'id' => type_integer(),
                            'more_ids' => type_list(type_map(type_string(), type_integer())),
                        ])),
                    ));
                }

                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(FlowContext $context, ?int $limit = null): Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield rows(
                            schema(list_schema(
                                'ids',
                                type_list(type_structure([
                                    'id' => type_integer(),
                                    'more_ids' => type_list(type_map(type_string(), type_integer())),
                                ])),
                            )),
                            row([
                                'ids' => [
                                    ['id' => $i + 1, 'more_ids' => [['more_id' => $i + 4], ['more_id' => $i + 7]]],
                                    ['id' => $i + 2, 'more_ids' => [['more_id' => $i + 5], ['more_id' => $i + 8]]],
                                    ['id' => $i + 3, 'more_ids' => [['more_id' => $i + 6], ['more_id' => $i + 9]]],
                                ],
                            ]),
                        );
                    }
                }
            })
            ->withEntries([
                'expanded' => ref('ids')->expand(),
                'element' => ref('expanded')->unpack(schema(
                    int_schema('id'),
                    list_schema('more_ids', type_list(type_map(type_string(), type_integer()))),
                )),
                'more_ids' => ref('element.more_ids')->expand(),
            ])
            ->rename('element.id', 'id')
            ->drop('expanded', 'ids', 'element', 'element.more_ids')
            ->limit(3)
            ->fetch();

        static::assertCount(3, $rows);
    }

    public function test_limit_with_batch_size(): void
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
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(FlowContext $context, ?int $limit = null): Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i + 1]), row(['id' => $i + 2]));
                    }
                }
            })
            ->batchSize(50)
            ->limit(10)
            ->fetch();

        static::assertCount(10, $rows);
    }

    public function test_limit_with_collecting(): void
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
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(FlowContext $context, ?int $limit = null): Generator
                {
                    for ($i = 0; $i < 100; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i + 1]), row(['id' => $i + 2]));
                    }
                }
            })
            ->limit(10)
            ->collect()
            ->fetch();

        static::assertCount(10, $rows);
    }

    public function test_with_total_rows_below_the_limit(): void
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
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, Signal|null, void>
                 */
                public function extract(FlowContext $context, ?int $limit = null): Generator
                {
                    for ($i = 0; $i < 5; $i++) {
                        yield rows(schema(integer_schema('id')), row(['id' => $i]));
                    }
                }
            })
            ->limit(10)
            ->fetch();

        static::assertCount(5, $rows);
    }

    public function test_limit_after_a_filter_returns_exactly_the_limit(): void
    {
        $source = from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 20)));

        static::assertSame(
            [2, 4, 6, 8, 10],
            array_column(
                df()
                    ->read($source)
                    ->select('id')
                    ->filter(ref('id')->mod(lit(2))->equals(lit(0)))
                    ->limit(5)
                    ->fetch()
                    ->toArray(),
                'id',
            ),
        );
        static::assertSame(
            [2, 4, 6, 8, 10],
            array_column(
                df()
                    ->read($source)
                    ->filter(ref('id')->mod(lit(2))->equals(lit(0)))
                    ->limit(5)
                    ->fetch()
                    ->toArray(),
                'id',
            ),
        );
    }

    public function test_a_limit_over_a_sort_runs_as_a_top_n_with_the_sorts_result(): void
    {
        $data = [];

        foreach (range(1, 50) as $i) {
            $data[] = ['id' => $i, 'group' => $i % 7];
        }

        $sorted = df()
            ->read(from_array($data))
            ->batchSize(4)
            ->sortBy([ref('group')->desc(), ref('id')])
            ->fetch()
            ->toArray();
        $frame = df()
            ->read(from_array($data))
            ->batchSize(4)
            ->sortBy([ref('group')->desc(), ref('id')])
            ->limit(5);

        static::assertSame(array_slice($sorted, 0, 5), $frame->fetch()->toArray());
        $plan = $frame->explain();
        static::assertContains(TopNProcessor::class, array_map(
            static fn(object $step): string => $step::class,
            (new Planner(Optimizer::default()))
                ->plan($plan->logical, $plan->context)
                ->root()
                ->segments()
                ->steps(),
        ));
    }

    public function test_offset_then_limit_returns_the_page(): void
    {
        static::assertSame(
            [['id' => 101], ['id' => 102], ['id' => 103]],
            df()
                ->read(from_array(array_map(static fn(int $i): array => ['id' => $i], range(1, 500))))
                ->offset(100)
                ->limit(3)
                ->fetch()
                ->toArray(),
        );
    }

    /**
     * @return Generator<string, array{bool}>
     */
    public static function read_frame_schemas(): Generator
    {
        yield 'derived schema' => [false];
        yield 'declared schema' => [true];
    }

    #[DataProvider('read_frame_schemas')]
    public function test_a_limit_over_a_read_frame_reaches_that_frames_source(bool $declared): void
    {
        $source = new RecordingFileExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), ...array_map(static fn(int $id) => row(['id' => $id]), range(1, 6))),
        );
        $nested = from_data_frame(df()->read($source));

        if ($declared) {
            $nested->withSchema(schema(int_schema('id')));
        }

        $rows = df()->read($nested)->offset(1)->limit(2)->fetch();

        static::assertSame([['id' => 2], ['id' => 3]], $rows->toArray());
        static::assertSame([3], $source->limits);
    }
}
