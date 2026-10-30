<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor\Detector;

use Flow\ETL\Adapter\CSV\IO\CSVDialect;

use function array_reduce;
use function count;
use function is_string;
use function round;
use function str_getcsv;
use function strlen;

final class Option
{
    private const int COLUMN_SCORE_WEIGHT = 100_000;

    private const int COLUMNS_LENGTH_WEIGHT = 10_000;

    /**
     * @var array<array<mixed>>
     */
    private array $rows;

    public function __construct(
        public string $separator,
        public string $enclosure,
        public string $escape = '\\',
    ) {
        CSVDialect::assertParts($this->separator, $this->enclosure, $this->escape);

        $this->rows = [];
    }

    public function isValid(): bool
    {
        $columnsCount = null;

        foreach ($this->rows as $row) {
            if ($columnsCount === null) {
                $columnsCount = count($row);

                continue;
            }

            if ($columnsCount !== count($row)) {
                return false;
            }
        }

        if ($columnsCount === 1) {
            return false;
        }

        return true;
    }

    public function parse(string $line): void
    {
        $this->rows[] = str_getcsv($line, $this->separator, $this->enclosure, '\\');
    }

    public function reset(): self
    {
        return new self($this->separator, $this->enclosure);
    }

    public function score(): int
    {
        if (!$this->isValid()) {
            return 0;
        }

        if (!count($this->rows)) {
            return 0;
        }

        $firstRow = $this->rows[0];

        $columnScore = count($firstRow) * self::COLUMN_SCORE_WEIGHT;
        $totalLength = array_reduce(
            $this->rows,
            static fn(int $carry, array $row): int => $carry
            + array_reduce(
                $row,
                static fn(int $carry, mixed $column): int => $carry + (is_string($column) ? strlen($column) : 0),
                0,
            ),
            0,
        );

        $lengthScore = (int) round((1 / ($totalLength + 1)) * self::COLUMNS_LENGTH_WEIGHT);

        return $columnScore + $lengthScore;
    }
}
