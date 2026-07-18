<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Row;
use Generator;

use function array_key_exists;
use function array_keys;
use function count;

final class HashTable
{
    /**
     * @var array<string, list<int>>
     */
    private array $buckets = [];

    /**
     * @var list<Row>
     */
    private array $rows = [];

    /**
     * @var array<int, true>
     */
    private array $unmatched = [];

    public function __construct(
        private readonly JoinKeys $keys,
        private readonly bool $trackUnmatched = false,
    ) {}

    public function add(Row $row): void
    {
        $index = count($this->rows);
        $this->rows[] = $row;
        $this->buckets[$this->keys->rightHash($row)][] = $index;

        if ($this->trackUnmatched) {
            $this->unmatched[$index] = true;
        }
    }

    /**
     * @return array<int, Row>
     */
    public function candidatesFor(Row $leftRow): array
    {
        $hash = $this->keys->leftHash($leftRow);

        if (!array_key_exists($hash, $this->buckets)) {
            return [];
        }

        $candidates = [];

        foreach ($this->buckets[$hash] as $index) {
            $candidates[$index] = $this->rows[$index];
        }

        return $candidates;
    }

    public function count(): int
    {
        return count($this->rows);
    }

    public function matched(int $index): void
    {
        unset($this->unmatched[$index]);
    }

    /**
     * @return Generator<Row>
     */
    public function unmatchedRows(): Generator
    {
        foreach (array_keys($this->unmatched) as $index) {
            yield $this->rows[$index];
        }
    }
}
