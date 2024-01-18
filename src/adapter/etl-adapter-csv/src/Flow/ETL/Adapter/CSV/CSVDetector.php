<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;

final class CSVDetector
{
    private $resource;

    private int $startingPosition;

    /**
     * @param $resource
     */
    public function __construct($resource)
    {
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('Argument must be a valid resource');
        }

        $this->resource = $resource;
        $this->startingPosition = \ftell($resource);
    }

    public function __destruct()
    {
        \fseek($this->resource, $this->startingPosition);
    }

    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     */
    public function separator(int $lines = 5) : string
    {
        if ($lines < 1) {
            throw new InvalidArgumentException('Lines must be greater than 0');
        }

        $delimiters = [
            ',' => [],
            "\t" => [],
            ';' => [],
            '|' => [],
            ' ' => [],
            '_' => [],
            '-' => [],
            ':' => [],
        ];

        $readLines = 1;

        while ($line = \fgets($this->resource)) {
            foreach ($delimiters as $delimiter => $count) {
                $row = \str_getcsv($line, $delimiter);
                $delimiters[$delimiter][] = \count($row);
            }

            if ($readLines++ >= $lines) {
                break;
            }
        }

        foreach ($delimiters as $delimiter => $rows) {
            $columnsCount = null;

            foreach ($rows as $rowColumns) {
                if ($columnsCount === null) {
                    $columnsCount = $rowColumns;
                }

                if ($columnsCount !== $rowColumns) {
                    unset($delimiters[$delimiter]);

                    break;
                }
            }
        }

        $delimiters = \array_map(fn (array $rows) : int => \array_sum($rows), $delimiters);

        \arsort($delimiters);

        $delimiters = \array_filter($delimiters, fn (int $count) : bool => $count > $lines);

        if (!\count($delimiters)) {
            \fseek($this->resource, $this->startingPosition);

            throw new RuntimeException('Cannot detect delimiter');
        }

        \fseek($this->resource, $this->startingPosition);

        return \array_key_first($delimiters);
    }
}
