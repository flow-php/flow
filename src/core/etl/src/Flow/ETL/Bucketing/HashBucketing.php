<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Generator;

use function count;
use function hexdec;
use function sprintf;
use function substr;

final class HashBucketing
{
    private readonly KeyValues $keyValues;

    /**
     * @param list<Reference> $by
     * @param int<1, max> $bucketsCount
     */
    public function __construct(
        array $by,
        private readonly int $bucketsCount,
        private readonly Hasher $hasher,
        private readonly RandomValueGenerator $random,
        private readonly string $namespace = 'bucket',
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }

        $this->keyValues = new KeyValues($by);
    }

    public function index(string $hash): int
    {
        return (int) hexdec(substr($hash, 0, 8)) % $this->bucketsCount;
    }

    /**
     * The bucket a row with these key values lands in.
     *
     * @param list<mixed> $values in the order of the bucketing references
     */
    public function indexOf(array $values): int
    {
        return $this->index($this->hasher->hash([$values])[0]);
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Bucket>
     */
    public function bucketize(Generator $rows, BucketsStorage $storage): Generator
    {
        $runId = $this->random->string(16);

        /** @var array<string, int> $totals */
        $totals = [];

        /** @var array<string, int> $indexes */
        $indexes = [];

        foreach ($rows as $batch) {
            $hashes = $this->hasher->hash($this->keyValues->of($batch));

            /** @var array<string, list<int>> $groups */
            $groups = [];

            foreach ($hashes as $i => $hash) {
                $index = $this->index($hash);
                $id = sprintf('%s-%s-%d', $this->namespace, $runId, $index);
                $groups[$id][] = $i;
                $indexes[$id] = $index;
            }

            // a bucket is a subset of a batch that already passed the gate, under the same schema
            foreach ($groups as $id => $indices) {
                $storage->append($id, $batch->gather($indices));
                $totals[$id] = ($totals[$id] ?? 0) + count($indices);
            }
        }

        foreach ($totals as $id => $totalRows) {
            yield new Bucket($id, $totalRows, $indexes[$id]);
        }
    }
}
