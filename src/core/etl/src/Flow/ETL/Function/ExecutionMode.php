<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

enum ExecutionMode
{
    case LENIENT;
    case STRICT;
}
