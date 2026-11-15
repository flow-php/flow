<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Grouping\KeyEncoder;
use Flow\ETL\Values\Rows;
use Generator;

use function sprintf;

final class HashBucketing
{
    /**
     * @param int<1, max> $bucketsCount 1 routes every row to bucket 0 without hashing
     * @param int<0, 24> $hashOffset the hex digit indexOf() reads 8 digits from: 0 for in-process spills, 8 for a
     *                               shuffle, so a shuffled bucket's own spill spreads over other buckets
     */
    public function __construct(
        private readonly KeyEncoder $keys,
        private readonly int $bucketsCount,
        private readonly RandomValueGenerator $random,
        private readonly string $namespace = 'bucket',
        private readonly int $hashOffset = 0,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }
    }

    /**
     * @param Generator<Rows> $rows
     * @param null|non-empty-string $runId null = random; given = the ids are "{namespace}-{runId}-{index}", the same
     *                                     in every process
     *
     * @return Generator<Bucket>
     */
    public function bucketize(Generator $rows, RowsStorage $storage, Backend $backend, ?string $runId = null): Generator
    {
        $runId ??= $this->random->string(16);

        /** @var array<int, int> $totals */
        $totals = [];

        foreach ($rows as $batch) {
            // a bucket is a subset of a batch that already passed the gate, under the same schema
            foreach ($backend->partition(
                $batch,
                $this->keys->columns($batch, $backend),
                $this->bucketsCount,
                $this->hashOffset,
            ) as $index => $part) {
                $storage->append(self::id($this->namespace, $runId, $index), $part);
                $totals[$index] = ($totals[$index] ?? 0) + $part->count();
            }
        }

        foreach ($totals as $index => $totalRows) {
            yield new Bucket(self::id($this->namespace, $runId, $index), $totalRows, $index);
        }
    }

    public static function id(string $namespace, string $runId, int $index): string
    {
        return sprintf('%s-%s-%d', $namespace, $runId, $index);
    }

    public function indexOf(string $key): int
    {
        return $this->bucketsCount === 1 ? 0 : KeyEncoder::bucketOf($key, $this->bucketsCount, $this->hashOffset);
    }
}
