<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\Hasher;
use Flow\ETL\Bucketing\KeyGrouping;
use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Column\Backend;
use Flow\ETL\Dataset\Memory\BoundedRead;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_slice;

/**
 * One batch per key. While the process stays under the memory limit the input is held and grouped in one pass; past
 * it, the input is partitioned into buckets and each bucket grouped on its own.
 */
final readonly class RepartitionProcessor implements Processor
{
    public function __construct(
        public References $by,
        public HashBucketing $bucketing,
        public Buckets $buckets,
        public Unit $memoryLimit,
        public Hasher $hasher = new NativeHasher(),
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $grouping = new KeyGrouping(new KeyValues($this->by->all()), $this->hasher, $context->backend());
        $bounded = new BoundedRead($this->memoryLimit, $context->backend());
        [$read, $fits] = $bounded->read($rows);

        try {
            if ($fits) {
                yield from $grouping->group($this->concatenated($read, $context->backend()));

                return;
            }

            foreach ($this->bucketing->bucketize(
                $bounded->followedBy($read, $rows),
                $this->buckets->storage(),
            ) as $bucket) {
                $this->buckets->add($bucket);
            }

            foreach ($this->buckets->all() as $bucket) {
                // re-key batches, yield from would restart keys at 0 for every bucket
                foreach ($grouping->group($this->buckets->rows($bucket->id)) as $group) {
                    yield $group;
                }
            }
        } finally {
            $this->buckets->clear();
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    /**
     * The held batches as one, so each key is gathered once rather than once per batch.
     *
     * @param list<Rows> $batches
     *
     * @return Generator<Rows>
     */
    public function concatenated(array $batches, Backend $backend): Generator
    {
        $parts = [];
        $schema = null;

        foreach ($batches as $batch) {
            if ($batch->isEmpty()) {
                continue;
            }

            $schema ??= $batch->schema();
            $parts[] = $batch->matchTo($schema, $backend);
        }

        if ($parts !== []) {
            yield $parts[0]->concat($backend, ...array_slice($parts, 1));
        }
    }
}
