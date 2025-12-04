<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\{ErrorHandler, Rows};

final class IgnoreError implements ErrorHandler
{
    #[\Override]
    public function skipRows(\Throwable $error, Rows $rows) : bool
    {
        return false;
    }

    #[\Override]
    public function throw(\Throwable $error, Rows $rows) : bool
    {
        return false;
    }
}
