<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter;

use Flow\ETL\Formatter;
use Flow\ETL\Formatter\ASCII\ASCIITable;
use Flow\ETL\Rows;

final class AsciiTableFormatter implements Formatter
{
    public function format(Rows $rows, int|bool $truncate = 20) : string
    {
        if ($rows->count() === 0) {
            return '';
        }

        return (new ASCIITable($rows))->print($truncate) . PHP_EOL
            . "{$rows->count()} rows" . PHP_EOL;
    }
}
