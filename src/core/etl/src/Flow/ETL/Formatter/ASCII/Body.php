<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Partition;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class Body
{
    public function __construct(private readonly Rows $rows)
    {
    }

    public function maximumLength(string $entry, int|bool $truncate = 20) : int
    {
        $max = 0;

        foreach ($this->rows as $row) {
            try {
                $value = new ASCIIValue($row->entries()->get($entry));

                if ($value->length($truncate) >= $max) {
                    $max = $value->length($truncate);
                }
            } catch (InvalidArgumentException $e) {
            }
        }

        return $max;
    }

    /**
     * @return Partition[]
     */
    public function partitions() : array
    {
        return $this->rows->partitions()->toArray();
    }

    /**
     * @return array<Row>
     */
    public function rows() : array
    {
        $rows = [];

        foreach ($this->rows as $row) {
            $rows[] = $row;
        }

        return $rows;
    }
}
