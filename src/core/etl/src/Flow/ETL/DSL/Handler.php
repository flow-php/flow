<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\ErrorHandler;

/**
 * @deprecated please use functions defined in Flow\ETL\DSL\functions.php
 *
 * @infection-ignore-all
 */
class Handler
{
    final public static function ignore_error() : ErrorHandler
    {
        return new ErrorHandler\IgnoreError();
    }

    final public static function skip_rows() : ErrorHandler
    {
        return new ErrorHandler\SkipRows();
    }

    final public static function throw_error() : ErrorHandler
    {
        return new ErrorHandler\ThrowError();
    }
}
