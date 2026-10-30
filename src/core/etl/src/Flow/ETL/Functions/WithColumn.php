<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

final readonly class WithColumn
{
    public function __construct(
        public string $name,
        public ScalarFunction $function,
    ) {}
}
