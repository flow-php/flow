<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Row, Rows};
use Flow\Filesystem\Partition;

final readonly class Body
{
    public function __construct(private Rows $rows)
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
            } catch (InvalidArgumentException) {
            }
        }

        return $max;
    }

    /**
     * @return array<Partition>
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
