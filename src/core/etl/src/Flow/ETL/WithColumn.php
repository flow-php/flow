<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Function\ScalarFunction;

final readonly class WithColumn
{
    public function __construct(
        public string $name,
        public ScalarFunction $function,
    ) {}
}
