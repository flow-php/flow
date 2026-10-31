<?php

declare(strict_types=1);

namespace Flow\ETL\API;

use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Transformer;

final readonly class TransformedBranch implements Branch
{
    public function __construct(
        private Transformer|Transformation $transformer,
        private Loader|Branch|Transactional $write,
    ) {}

    public function write(DataFrame $prefix): DataFrame
    {
        return $prefix->with($this->transformer)->write($this->write);
    }
}
