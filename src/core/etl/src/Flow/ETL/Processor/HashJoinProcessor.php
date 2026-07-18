<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Exception\JoinException;
use Flow\ETL\FlowContext;
use Flow\ETL\Join\BucketedHashJoin;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Processor;
use Generator;

/**
 * Performs hash join between upstream data and a DataFrame, spilling through the buckets
 * cache configured in the join config - in-memory (default) or on-disk.
 *
 * @internal
 */
final readonly class HashJoinProcessor implements Processor
{
    public function __construct(
        private DataFrame $right,
        private Expression $expression,
        private Join $join,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        try {
            yield from (new BucketedHashJoin(
                $this->right,
                $this->expression,
                $this->join,
                $context->config->join->cache,
                $context->config->join->bucketsCount,
                $context->config->join->batchSize,
            ))->joinGenerator($rows, $context);
        } catch (DuplicatedEntriesException $e) {
            throw new JoinException($e->getMessage(), (int) $e->getCode(), $e);
        }
    }
}
