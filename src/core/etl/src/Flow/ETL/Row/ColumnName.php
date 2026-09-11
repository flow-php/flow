<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use function is_int;
use function str_pad;

use const STR_PAD_LEFT;

final class ColumnName
{
    /**
     * An int-keyed row is a positional record, not a named one, so position N becomes "eNN". A plain
     * (string) cast cannot express this: PHP coerces a numeric string array key straight back to int.
     * A string key is returned as-is - aliasing '' onto e00 would silently merge it with position 0.
     */
    public function of(int|string $key): string
    {
        if (is_int($key)) {
            return 'e' . str_pad((string) $key, 2, '0', STR_PAD_LEFT);
        }

        return $key;
    }
}
