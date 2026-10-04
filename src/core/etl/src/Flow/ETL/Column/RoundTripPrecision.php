<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Exception\RuntimeException;

use function ini_get;
use function ini_set;

final class RoundTripPrecision
{
    private ?string $previous = null;

    public function force(): void
    {
        $current = ini_get('serialize_precision');

        if ($current === '-1') {
            return;
        }

        if (ini_set('serialize_precision', '-1') === false) {
            throw new RuntimeException(
                'Writing floats requires serialize_precision = -1 and ini_set() cannot change it',
            );
        }

        $this->previous = (string) $current;
    }

    public function restore(): void
    {
        if ($this->previous !== null) {
            ini_set('serialize_precision', $this->previous);
            $this->previous = null;
        }
    }
}
