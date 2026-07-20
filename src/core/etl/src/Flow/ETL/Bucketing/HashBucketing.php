<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

use function array_map;
use function bin2hex;
use function hexdec;
use function random_bytes;
use function sprintf;
use function substr;

final class HashBucketing implements BucketingStrategy
{
    private readonly Hasher $hasher;

    private readonly KeyValues $keyValues;

    public function __construct(
        private readonly References $by,
        private readonly int $bucketsCount,
        ?Hasher $hasher = null,
        private readonly string $namespace = 'bucket',
    ) {
        if ($this->bucketsCount < 1) {
            throw new InvalidArgumentException('Buckets count must be greater than 0, given: ' . $this->bucketsCount);
        }

        $this->hasher = $hasher ?? new NativeHasher();
        $this->keyValues = new KeyValues($by);
    }

    public function by(): References
    {
        return $this->by;
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<BucketChunk>
     */
    public function bucketize(Generator $rows): Generator
    {
        $runId = bin2hex(random_bytes(8));

        foreach ($rows as $batch) {
            /** @var list<RowKey> $keys */
            $keys = [];

            foreach ($batch as $row) {
                $keys[] = new RowKey($row, $this->keyValues->ofRow($row));
            }

            $hashes = $this->hasher->hash(array_map(static fn(RowKey $key): array => $key->values, $keys));

            /** @var array<string, array{rows: list<Row>, hashes: list<string>, values: list<array<string, mixed>>}> $groups */
            $groups = [];

            foreach ($keys as $i => $key) {
                $hash = $hashes[$i];
                $id = sprintf(
                    '%s-%s-%d',
                    $this->namespace,
                    $runId,
                    (int) hexdec(substr($hash, 0, 8)) % $this->bucketsCount,
                );
                $groups[$id]['rows'][] = $key->row;
                $groups[$id]['hashes'][] = $hash;
                $groups[$id]['values'][] = $key->values;
            }

            foreach ($groups as $id => $group) {
                yield new BucketChunk($id, new Rows(...$group['rows']), $group['hashes'], $group['values']);
            }
        }
    }

    public function sortedBy(): ?References
    {
        return null;
    }
}
