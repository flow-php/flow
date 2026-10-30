<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Rows;
use Throwable;

final readonly class TransformationError
{
    public function __construct(
        public Throwable $cause,
        public Transformer $transformer,
        public Rows $rows,
    ) {}
}
