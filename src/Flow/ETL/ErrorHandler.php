<?php declare(strict_types=1);

namespace Flow\ETL;

interface ErrorHandler
{
    public function throw(\Throwable $error, Rows $rows) : bool;

    public function skipRows(\Throwable $error, Rows $rows) : bool;
}
