<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

use function array_splice;
use function bin2hex;
use function count;
use function random_bytes;

final class SortedRunBucketing implements BucketingStrategy
{
    private readonly Hasher $hasher;

    private readonly KeyValues $keyValues;

    public function __construct(
        private readonly References $refs,
        private readonly int $runSize,
        ?Hasher $hasher = null,
    ) {
        if ($this->runSize < 1) {
            throw new InvalidArgumentException('Run size must be greater than 0, given: ' . $this->runSize);
        }

        $this->hasher = $hasher ?? new NativeHasher();
        $this->keyValues = new KeyValues($refs);
    }

    public function by(): References
    {
        return $this->refs;
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<BucketChunk>
     */
    public function bucketize(Generator $rows): Generator
    {
        $toChunk = function (array $run): BucketChunk {
            /** @var list<Row> $run */
            $sorted = (new Rows(...$run))->sortBy(...$this->refs->all());
            $values = $this->keyValues->of($sorted);

            return new BucketChunk(bin2hex(random_bytes(16)), $sorted, $this->hasher->hash($values), $values);
        };

        /** @var list<Row> $buffer */
        $buffer = [];

        foreach ($rows as $batch) {
            foreach ($batch as $row) {
                $buffer[] = $row;
            }

            while (count($buffer) >= $this->runSize) {
                yield $toChunk(array_splice($buffer, 0, $this->runSize));
            }
        }

        if ($buffer !== []) {
            yield $toChunk($buffer);
        }
    }

    public function sortedBy(): ?References
    {
        return $this->refs;
    }
}
