<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

use function array_map;
use function count;
use function hexdec;
use function sprintf;
use function substr;

final class HashBucketing implements BucketingStrategy
{
    private readonly KeyValues $keyValues;

    public function __construct(
        private readonly References $by,
        private readonly int $bucketsCount,
        private readonly Hasher $hasher,
        private readonly RandomValueGenerator $random,
        private readonly string $namespace = 'bucket',
    ) {
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }

        $this->keyValues = new KeyValues($by);
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

        foreach ($rows as $batch) {
            /** @var list<RowKey> $keys */
            $keys = [];

            foreach ($batch as $row) {
                $keys[] = new RowKey($row, $this->keyValues->ofRow($row));
            }

            $hashes = $this->hasher->hash(array_map(static fn(RowKey $key): array => $key->values, $keys));

            /** @var array<string, list<Row>> $groups */
            $groups = [];

            foreach ($keys as $i => $key) {
                $id = sprintf(
                    '%s-%s-%d',
                    $this->namespace,
                    $runId,
                    (int) hexdec(substr($hashes[$i], 0, 8)) % $this->bucketsCount,
                );
                $groups[$id][] = $key->row;
            }

            foreach ($groups as $id => $groupRows) {
                $storage->append($id, new Rows(...$groupRows));
                $totals[$id] = ($totals[$id] ?? 0) + count($groupRows);
            }
        }

        foreach ($totals as $id => $totalRows) {
            yield new Bucket($id, $totalRows);
        }
    }
}
