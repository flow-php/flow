<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class LimitTest extends FlowIntegrationTestCase
{
    public function test_exceeding_the_limit_in_one_rows_set(): void
    {
        $rows = df()
            ->read(from_array(\array_map(static fn(int $id): array => ['id' => $id], \range(1, 1000))))
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

        df()->read(from_rows(\Flow\ETL\DSL\rows()))->fetch(-1);
    }

    public function test_fetch_without_limit(): void
    {
        $rows = df()->read(new class implements Extractor {
            /**
             * @param FlowContext $context
             *
             * @return \Generator<int, Rows, mixed, void>
             */
            public function extract(FlowContext $context): \Generator
            {
                for ($i = 0; $i < 20; $i++) {
                    yield \Flow\ETL\DSL\rows(row(integer_entry('id', $i)));
                }
            }
        })->fetch();

        static::assertCount(20, $rows);
    }

    public function test_limit(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield \Flow\ETL\DSL\rows(row(integer_entry('id', $i + 1)), row(integer_entry('id', $i + 2)));
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

        df()->read(from_rows(\Flow\ETL\DSL\rows()))->limit(-1);
    }

    public function test_limit_null(): void
    {
        $rows = df()
            ->read(from_array(\array_map(static fn(int $id): array => ['id' => $id], \range(1, 10))))
            ->limit(null)
            ->fetch();

        static::assertCount(10, $rows);
    }

    public function test_limit_when_transformation_is_expanding_rows_extracted_from_extractor(): void
    {
        $rows = df()
            ->read(new class implements Extractor {
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield \Flow\ETL\DSL\rows(row(list_entry(
                            'ids',
                            [
                                ['id' => $i + 1, 'more_ids' => [['more_id' => $i + 4], ['more_id' => $i + 7]]],
                                ['id' => $i + 2, 'more_ids' => [['more_id' => $i + 5], ['more_id' => $i + 8]]],
                                ['id' => $i + 3, 'more_ids' => [['more_id' => $i + 6], ['more_id' => $i + 9]]],
                            ],
                            type_list(type_structure([
                                'id' => type_integer(),
                                'more_ids' => type_list(type_map(type_string(), type_integer())),
                            ])),
                        )));
                    }
                }
            })
            ->withEntries([
                'expanded' => ref('ids')->expand(),
                'element' => ref('expanded')->unpack(),
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
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 0; $i < 1000; $i++) {
                        yield \Flow\ETL\DSL\rows(row(integer_entry('id', $i + 1)), row(integer_entry('id', $i + 2)));
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
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 0; $i < 100; $i++) {
                        yield \Flow\ETL\DSL\rows(row(integer_entry('id', $i + 1)), row(integer_entry('id', $i + 2)));
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
                /**
                 * @param FlowContext $context
                 *
                 * @return \Generator<int, Rows, mixed, void>
                 */
                public function extract(FlowContext $context): \Generator
                {
                    for ($i = 0; $i < 5; $i++) {
                        yield \Flow\ETL\DSL\rows(row(integer_entry('id', $i)));
                    }
                }
            })
            ->limit(10)
            ->fetch();

        static::assertCount(5, $rows);
    }
}
