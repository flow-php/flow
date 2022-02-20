<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter;

use Flow\ETL\Formatter;
use Flow\ETL\Formatter\ASCII\ASCIITable;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class AsciiTableFormatter implements Formatter
{
    /**
     * @psalm-suppress UnusedMethodCall
     * @psalm-suppress MixedArrayAssignment
     * @psalm-suppress MixedArgument
     */
    public function format(Rows $rows, int $truncate = 20) : string
    {
        if ($rows->count() === 0) {
            return '';
        }

        $array = [];

        $rows->each(function (Row $row) use (&$array) : void {
            $rowsArray = [];

            foreach ($row->entries()->all() as $entry) {
                $rowsArray[$entry->name()] = (string) $entry;
            }

            $array[] = $rowsArray;
        });

        return (new ASCIITable())->makeTable($array, $truncate)
            . "{$rows->count()} rows\n";
    }
}
