<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\ErrorHandler;

use Flow\ETL\IO\Loader;
use Flow\ETL\Values\Rows;
use Throwable;

final readonly class LoadingError
{
    public function __construct(
        public Throwable $cause,
        public Loader $loader,
        public Rows $rows,
    ) {}
}
