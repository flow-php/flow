<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Rows;
use Generator;

use function count;

final readonly class BucketAggregation
{
    private const int RESULT_BATCH_SIZE = 1000;

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function aggregate(Generator $rows, FlowContext $context, GroupBy $groupBy): Generator
    {
        /** @var array<string, Bucket> $buckets */
        $buckets = [];
        $aggregations = $groupBy->aggregations();

        foreach ($rows as $batch) {
            foreach ($batch as $row) {
                $key = $groupBy->keyValues($row);
                $bucket = $buckets[(string) $key] ??= new Bucket($key, $aggregations->cloned());
                $bucket->aggregators->aggregate($row, $context);
            }
        }

        $buffer = [];
        $entryFactory = $context->entryFactory();

        foreach ($buckets as $bucket) {
            $buffer[] = $groupBy->aggregatedRow($bucket->key, $bucket->aggregators, $entryFactory);

            if (count($buffer) >= self::RESULT_BATCH_SIZE) {
                yield new Rows(...$buffer);
                $buffer = [];
            }
        }

        if ($buffer !== []) {
            yield new Rows(...$buffer);
        }
    }
}
