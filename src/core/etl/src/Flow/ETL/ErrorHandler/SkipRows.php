<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Rows;

final class SkipRows implements ErrorHandler
{
    public function skipRows(\Throwable $error, Rows $rows) : bool
    {
        return true;
    }

    public function throw(\Throwable $error, Rows $rows) : bool
    {
        return false;
    }
}
