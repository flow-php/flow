<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Operators\Transformer;

interface Limiting extends Transformer
{
    public function reached(): bool;
}
