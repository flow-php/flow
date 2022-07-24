<?php declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row;

/**
 * @psalm-immutable
 */
interface Comparator
{
    public function equals(Row $row, Row $nextRow) : bool;
}
