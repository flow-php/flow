<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

enum Style : string
{
    case LOWER = 'LOWER';
    case UCFIRST = 'UCFIRST';
    case UCWORDS = 'UCWORDS';
    case UPPER = 'UPPER';
}
