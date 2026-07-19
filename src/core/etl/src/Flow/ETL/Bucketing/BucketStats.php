<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;

use function array_key_exists;

final readonly class BucketStats
{
    /**
     * @param array<string, mixed> $min by-column => min non-null value (null when the column had only nulls)
     * @param array<string, mixed> $max by-column => max non-null value (null when the column had only nulls)
     * @param array<string, int> $nullCounts by-column => count of null values
     */
    public function __construct(
        private int $rowsCount,
        private int $chunksCount,
        private array $min,
        private array $max,
        private array $nullCounts,
        private ?int $distinctEstimate,
        private bool $distinctExact,
        private ?References $sortedBy = null,
    ) {}

    public function chunksCount(): int
    {
        return $this->chunksCount;
    }

    public function distinctEstimate(): ?int
    {
        return $this->distinctEstimate;
    }

    public function distinctIsExact(): bool
    {
        return $this->distinctExact;
    }

    public function max(string $column): mixed
    {
        if (!array_key_exists($column, $this->max)) {
            throw new InvalidArgumentException("No max statistic tracked for column \"{$column}\".");
        }

        return $this->max[$column];
    }

    public function min(string $column): mixed
    {
        if (!array_key_exists($column, $this->min)) {
            throw new InvalidArgumentException("No min statistic tracked for column \"{$column}\".");
        }

        return $this->min[$column];
    }

    public function nullCount(string $column): int
    {
        return $this->nullCounts[$column] ?? 0;
    }

    public function rowsCount(): int
    {
        return $this->rowsCount;
    }

    /**
     * rowsCount / distinctEstimate - skew indicator; null when distinct is unknown or zero.
     */
    public function skew(): ?float
    {
        if ($this->distinctEstimate === null || $this->distinctEstimate === 0) {
            return null;
        }

        return $this->rowsCount / $this->distinctEstimate;
    }

    public function sortedBy(): ?References
    {
        return $this->sortedBy;
    }
}
