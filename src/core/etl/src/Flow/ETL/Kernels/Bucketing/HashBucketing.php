<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Rows;
use Generator;

use function array_merge;
use function count;
use function hash;
use function hexdec;
use function sort;
use function sprintf;
use function substr;

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
     * Distinct keys only: KeyGrouping groups a batch's rows by key first, so xxh128 runs once per distinct key.
     *
     * @param Generator<Rows> $rows
     * @param null|non-empty-string $runId null = random; given = the ids are "{namespace}-{runId}-{index}", the same
     *                                     in every process
     *
     * @return Generator<Bucket>
     */
    public function bucketize(Generator $rows, RowsStorage $storage, ?string $runId = null): Generator
    {
        $runId ??= $this->random->string(16);

        /** @var array<int, int> $totals */
        $totals = [];

        foreach ($rows as $batch) {
            /** @var array<int, list<list<int>>> $groups */
            $groups = [];

            foreach (KeyGrouping::indices($this->keys->of($batch)) as $key => $rowsOfKey) {
                $groups[$this->indexOf($key)][] = $rowsOfKey;
            }

            // a bucket is a subset of a batch that already passed the gate, under the same schema
            foreach ($groups as $index => $parts) {
                $indices = array_merge(...$parts);
                // in row order, as the batch held them
                sort($indices);
                $storage->append(self::id($this->namespace, $runId, $index), $batch->gather($indices));
                $totals[$index] = ($totals[$index] ?? 0) + count($indices);
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
        return $this->bucketsCount === 1
            ? 0
            : (int) hexdec(substr(hash('xxh128', $key), $this->hashOffset, 8)) % $this->bucketsCount;
    }
}
