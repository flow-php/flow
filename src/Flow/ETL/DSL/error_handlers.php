<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\ErrorHandler;

use Flow\ETL\ErrorHandler;

function ignore_error() : ErrorHandler
{
    return new ErrorHandler\IgnoreError();
}

function skip_rows() : ErrorHandler
{
    return new ErrorHandler\SkipRows();
}

function throw_error() : ErrorHandler
{
    return new ErrorHandler\ThrowError();
}
