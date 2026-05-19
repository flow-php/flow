<?php

declare(strict_types=1);

namespace Flow\ETL;

use Throwable;

interface ErrorHandler
{
    public function skipRows(Throwable $error, Rows $rows): bool;

    public function throw(Throwable $error, Rows $rows): bool;
}
