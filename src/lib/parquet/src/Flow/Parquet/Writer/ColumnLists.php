<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;

use function array_unique;
use function count;
use function implode;
use function max;

final readonly class ColumnLists
{
    /**
     * @param array<string, list<mixed>> $columns
     *
     * @throws InvalidArgumentException when the lists differ in length
     */
    public function length(array $columns): int
    {
        $lengths = [];

        foreach ($columns as $name => $values) {
            $lengths[$name] = count($values);
        }

        if (count(array_unique($lengths)) > 1) {
            $described = [];

            foreach ($lengths as $name => $length) {
                $described[] = '"' . $name . '": ' . $length;
            }

            throw new InvalidArgumentException(
                'writeColumns() takes lists of one length, got ' . implode(', ', $described),
            );
        }

        return $lengths === [] ? 0 : max($lengths);
    }
}
