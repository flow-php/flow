<?php

declare(strict_types=1);

namespace Flow\ETL\ErrorHandler;

use Flow\ETL\{ErrorHandler, Rows};

final class SkipRows implements ErrorHandler
{
    #[\Override]
    public function skipRows(\Throwable $error, Rows $rows) : bool
    {
        return true;
    }

    #[\Override]
    public function throw(\Throwable $error, Rows $rows) : bool
    {
        return false;
    }
}
