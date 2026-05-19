<?php

declare(strict_types=1);

namespace Flow\Dremel;

use RuntimeException;

final class Dremel
{
    public function __construct()
    {
        throw new RuntimeException(
            'Independent Dremel implementation is not yet available, please fallback to flow-php/parquet library DremelShredder/DremelAssembler classes',
        );
    }
}
