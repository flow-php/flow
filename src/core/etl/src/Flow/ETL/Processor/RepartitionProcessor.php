<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Bucketing\Hasher;
use Flow\ETL\Bucketing\KeyGrouping;
use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

use function Flow\Types\DSL\type_string;

/**
 * Regroups buckets spilled by BucketingProcessor so every row sharing a key arrives in one batch.
 *
 * @internal
 */
final readonly class RepartitionProcessor implements Processor
{
    public function __construct(
        private References $by,
        private Buckets $buckets,
        private Hasher $hasher = new NativeHasher(),
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $grouping = new KeyGrouping(new KeyValues($this->by->all()), $this->hasher);

        try {
            foreach ($rows as $metadata) {
                foreach ($metadata as $row) {
                    $bucketId = type_string()->assert($row->get(BucketShape::id->value));

                    // re-key batches, yield from would restart keys at 0 for every bucket
                    foreach ($grouping->group($this->buckets->rows($bucketId)) as $group) {
                        yield $group;
                    }
                }
            }
        } finally {
            $this->buckets->clear();
        }
    }
}
