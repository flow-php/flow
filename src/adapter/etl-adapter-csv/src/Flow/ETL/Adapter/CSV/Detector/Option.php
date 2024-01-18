<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Detector;

use Flow\ETL\Exception\InvalidArgumentException;

final class Option
{
    private const COLUMN_SCORE_WEIGHT = 100_000;

    private const COLUMNS_LENGTH_WEIGHT = 10_000;

    /**
     * @var array<mixed>
     */
    private array $rows;

    public function __construct(
        public string $separator,
        public string $enclosure,
        public string $escape = '\\'
    ) {
        if (\mb_strlen($this->separator) !== 1) {
            throw new InvalidArgumentException('Separator must be a single character');
        }

        if (\mb_strlen($this->enclosure) !== 1) {
            throw new InvalidArgumentException('Enclosure must be a single character');
        }

        $this->rows = [];
    }

    public function isValid() : bool
    {
        $columnsCount = null;

        foreach ($this->rows as $row) {
            if ($columnsCount === null) {
                $columnsCount = \count($row);

                continue;
            }

            if ($columnsCount !== \count($row)) {
                return false;
            }
        }

        if ($columnsCount === 1) {
            return false;
        }

        return true;
    }

    public function parse(string $line) : void
    {
        $this->rows[] = \str_getcsv($line, $this->separator, $this->enclosure);
    }

    public function reset() : self
    {
        return new self($this->separator, $this->enclosure);
    }

    public function score() : int
    {
        if (!$this->isValid()) {
            return 0;
        }

        if (!\count($this->rows)) {
            return 0;
        }

        $columnScore = \count($this->rows[0]) * self::COLUMN_SCORE_WEIGHT;
        $totalLength = \array_reduce(
            $this->rows,
            static fn (int $carry, array $row) : int => $carry + \array_reduce(
                $row,
                static fn (int $carry, $column) : int => $carry + (\is_string($column) ? \mb_strlen($column) : 0),
                0
            ),
            0
        );

        $lengthScore = (int) \round((1 / ($totalLength + 1) * self::COLUMNS_LENGTH_WEIGHT));

        return $columnScore + $lengthScore;
    }
}
