<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\StyleConverter;

enum Style : string
{
    case LOWER = 'LOWER';
    case UCFIRST = 'UCFIRST';
    case UCWORDS = 'UCWORDS';
    case UPPER = 'UPPER';
}
