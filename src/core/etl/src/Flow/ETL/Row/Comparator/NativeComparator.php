<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Comparator;

use Flow\ETL\Row;
use Flow\ETL\Row\Comparator;

/**
 * @psalm-immutable
 */
final class NativeComparator implements Comparator
{
    public function equals(Row $row, Row $nextRow) : bool
    {
        return $row->isEqual($nextRow);
    }
}
