<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Generator;

use function count;
use function hexdec;
use function sprintf;
use function substr;

final class HashBucketing implements BucketingStrategy
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
        bool $nullOnMissing = false,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }

        $this->keyValues = new KeyValues($by, $nullOnMissing);
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

            /** @var array<string, list<Row>> $groups */
            $groups = [];

            foreach ($batch as $i => $row) {
                $index = (int) hexdec(substr($hashes[$i], 0, 8)) % $this->bucketsCount;
                $id = sprintf('%s-%s-%d', $this->namespace, $runId, $index);
                $groups[$id][] = $row;
                $indexes[$id] = $index;
            }

            foreach ($groups as $id => $groupRows) {
                $storage->append($id, new Rows(...$groupRows));
                $totals[$id] = ($totals[$id] ?? 0) + count($groupRows);
            }
        }

        foreach ($totals as $id => $totalRows) {
            yield new Bucket($id, $totalRows, $indexes[$id]);
        }
    }
}
