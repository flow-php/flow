<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use PHPUnit\Framework\Attributes\TestWith;

use function ceil;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function min;

final class LimitBatchingTest extends FlowTestCase
{
    #[TestWith([1, 1])]
    #[TestWith([1, 5])]
    #[TestWith([3, 5])]
    #[TestWith([1000, 5])]
    #[TestWith([1000, 999])]
    #[TestWith([1000, 1000])]
    #[TestWith([1000, 1001])]
    public function test_limit_returns_exactly_n_rows_for_any_batch_size(int $batchSize, int $limit): void
    {
        static::assertSame(
            min($limit, 2000),
            data_frame()
                ->read((new CountingExtractor(
                    schema(int_schema('id')),
                    RowsMother::sequentialIds(2000),
                ))->withBatchSize($batchSize))
                ->limit($limit)
                ->fetch()
                ->count(),
        );
    }

    #[TestWith([1, 1])]
    #[TestWith([1, 5])]
    #[TestWith([3, 5])]
    #[TestWith([1000, 5])]
    #[TestWith([1000, 999])]
    #[TestWith([1000, 1000])]
    #[TestWith([1000, 1001])]
    public function test_limit_stops_the_source_within_one_batch(int $batchSize, int $limit): void
    {
        $extractor = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(2000)))->withBatchSize(
            $batchSize,
        );

        data_frame()->read($extractor)->limit($limit)->fetch();

        static::assertSame((int) ceil($limit / $batchSize), $extractor->batchesYielded);
    }
}
