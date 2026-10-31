<?php

declare(strict_types=1);

namespace Flow\ETL\API;

use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Loader;

final readonly class FilteredBranch implements Branch
{
    public function __construct(
        private ScalarFunction $condition,
        private Loader|Branch|Transactional $write,
    ) {}

    public function write(DataFrame $prefix): DataFrame
    {
        return $prefix->filter($this->condition)->write($this->write);
    }
}
