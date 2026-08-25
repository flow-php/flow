<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

/**
 * How functions behave when they encounter unexpected data - type mismatches, missing values.
 */
enum ExecutionMode
{
    case LENIENT;
    case STRICT;
}
