<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Tests\Context\PipelineStepList;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_column;
use function array_map;
use function array_slice;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
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
        static::assertCount(
            9,
            df()
                ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 1000))))
                ->limit(9)
                ->fetch(),
        );
    }

    public function test_fetch_with_limit(): void
    {
        static::assertCount(
            5,
            df()
                ->read(from_array([
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
                ->fetch(5),
        );
    }

    public function test_fetch_with_limit_below_0(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        df()->read(from_rows(rows(schema())))->fetch(-1);
    }

    public function test_fetch_without_limit(): void
    {
        static::assertCount(
            20,
            df()->read(new class implements Extractor {
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
                    for ($i = 0; $i < 20; $i++) {
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
            })->fetch(),
        );
    }

    public function test_limit(): void
    {
        static::assertCount(
            10,
            df()
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
                        for ($i = 0; $i < 1000; $i++) {
                            yield array_to_rows([['id' => $i + 1], ['id' => $i + 2]], schema(integer_schema('id')));
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
                ->limit(10)
                ->fetch(),
        );
    }

    public function test_limit_below_0(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Limit can't be lower or equal zero, given: -1");

        df()->read(from_rows(rows(schema())))->limit(-1);
    }

    public function test_limit_null(): void
    {
        static::assertCount(
            10,
            df()
                ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 10))))
                ->limit(null)
                ->fetch(),
        );
    }

    public function test_limit_when_transformation_is_expanding_rows_extracted_from_extractor(): void
    {
        static::assertCount(
            3,
            df()
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
                     * @return \Generator<int, Rows, Signal|null, void>
                     */
                    public function extract(Backend $backend, Criteria $criteria): Generator
                    {
                        for ($i = 0; $i < 1000; $i++) {
                            yield array_to_rows([[
                                'ids' => [
                                    ['id' => $i + 1, 'more_ids' => [['more_id' => $i + 4], ['more_id' => $i + 7]]],
                                    ['id' => $i + 2, 'more_ids' => [['more_id' => $i + 5], ['more_id' => $i + 8]]],
                                    ['id' => $i + 3, 'more_ids' => [['more_id' => $i + 6], ['more_id' => $i + 9]]],
                                ],
                            ]], schema(list_schema('ids', type_list(type_structure([
                                'id' => type_integer(),
                                'more_ids' => type_list(type_map(type_string(), type_integer())),
                            ])))));
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
                ->withColumns([
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
                ->fetch(),
        );
    }

    public function test_limit_with_batch_size(): void
    {
        static::assertCount(
            10,
            df()
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
                        for ($i = 0; $i < 1000; $i++) {
                            yield array_to_rows([['id' => $i + 1], ['id' => $i + 2]], schema(integer_schema('id')));
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
                ->batchSize(50)
                ->limit(10)
                ->fetch(),
        );
    }

    public function test_limit_with_collecting(): void
    {
        static::assertCount(
            10,
            df()
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
                        for ($i = 0; $i < 100; $i++) {
                            yield array_to_rows([['id' => $i + 1], ['id' => $i + 2]], schema(integer_schema('id')));
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
                ->limit(10)
                ->collect()
                ->fetch(),
        );
    }

    public function test_with_total_rows_below_the_limit(): void
    {
        static::assertCount(
            5,
            df()
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
                        for ($i = 0; $i < 5; $i++) {
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
                ->limit(10)
                ->fetch(),
        );
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
        static::assertContains(
            TopNProcessor::class,
            PipelineStepList::classes($plan->planner->plan($plan->logical)->root()),
        );
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

    public function test_an_offset_and_a_limit_reach_the_source(): void
    {
        $source = new RecordingFileExtractor(
            schema(int_schema('id')),
            array_to_rows(array_map(static fn(int $id) => ['id' => $id], range(1, 6)), schema(int_schema('id'))),
        );

        static::assertSame([['id' => 2], ['id' => 3]], df()->read($source)->offset(1)->limit(2)->fetch()->toArray());
        static::assertCount(1, $source->criteria);
        static::assertSame(1, $source->criteria[0]->offset);
        static::assertSame(2, $source->criteria[0]->limit);
    }
}
