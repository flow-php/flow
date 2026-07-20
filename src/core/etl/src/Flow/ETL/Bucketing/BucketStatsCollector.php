<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;

use function array_key_exists;
use function count;
use function is_numeric;
use function is_string;

final class BucketStatsCollector
{
    private int $chunksCount = 0;

    /**
     * @var array<string, true>
     */
    private array $distinct = [];

    private bool $distinctExact = true;

    /**
     * @var array<string, mixed>
     */
    private array $max = [];

    /**
     * @var array<string, mixed>
     */
    private array $min = [];

    /**
     * @var array<string, int>
     */
    private array $nullCounts = [];

    private int $rowsCount = 0;

    public function __construct(
        private readonly References $by,
        private readonly int $distinctCap = 10_000,
    ) {
        if ($this->distinctCap < 1) {
            throw new InvalidArgumentException('Distinct cap must be greater than 0, given: ' . $this->distinctCap);
        }

        foreach ($this->by as $ref) {
            $this->min[$ref->name()] = null;
            $this->max[$ref->name()] = null;
            $this->nullCounts[$ref->name()] = 0;
        }
    }

    public function bucket(string $id, ?References $sortedBy = null): Bucket
    {
        return new Bucket(
            $id,
            $this->by,
            new BucketStats(
                $this->rowsCount,
                $this->chunksCount,
                $this->min,
                $this->max,
                $this->nullCounts,
                count($this->distinct),
                $this->distinctExact,
                $sortedBy,
            ),
        );
    }

    /**
     * @param list<array<string, mixed>> $values bucket-key values, one map per row
     * @param list<string> $hashes bucket-key hashes aligned 1:1 with $values
     */
    public function collect(array $values, array $hashes): void
    {
        $this->chunksCount++;

        foreach ($values as $i => $rowValues) {
            $this->rowsCount++;

            $key = $hashes[$i];

            if (!array_key_exists($key, $this->distinct)) {
                if (count($this->distinct) < $this->distinctCap) {
                    $this->distinct[$key] = true;
                } else {
                    $this->distinctExact = false;
                }
            }

            foreach ($this->by as $ref) {
                $name = $ref->name();
                /** @var mixed $value */
                $value = $rowValues[$name];

                if ($value === null) {
                    $this->nullCounts[$name]++;

                    continue;
                }

                /** @var mixed $currentMin */
                $currentMin = $this->min[$name];

                if ($currentMin === null || self::compare($value, $currentMin) < 0) {
                    $this->min[$name] = $value;
                }

                /** @var mixed $currentMax */
                $currentMax = $this->max[$name];

                if ($currentMax === null || self::compare($value, $currentMax) > 0) {
                    $this->max[$name] = $value;
                }
            }
        }
    }

    private static function compare(mixed $a, mixed $b): int
    {
        if (is_numeric($a) && is_numeric($b)) {
            return (float) $a <=> (float) $b;
        }

        if (is_string($a) && is_string($b)) {
            return $a <=> $b;
        }

        if ($a instanceof DateTimeInterface && $b instanceof DateTimeInterface) {
            return $a <=> $b;
        }

        return 0;
    }
}
