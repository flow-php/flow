<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Throwable;

final readonly class TransformationError
{
    public function __construct(
        public Throwable $cause,
        public Transformer $transformer,
        public Rows $rows,
    ) {}
}
