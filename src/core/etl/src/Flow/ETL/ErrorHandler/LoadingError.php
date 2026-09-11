<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Throwable;

final readonly class LoadingError
{
    public function __construct(
        public Throwable $cause,
        public Loader $loader,
        public Rows $rows,
    ) {}
}
