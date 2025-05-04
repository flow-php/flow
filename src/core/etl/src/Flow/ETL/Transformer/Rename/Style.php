<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

enum Style : string
{
    case ASCII = 'ASCII';
    case CAMEL = 'CAMEL';
    case LOWER = 'LOWER';
    case SLUG = 'SLUG';
    case TITLE = 'TITLE';
    case UCFIRST = 'UCFIRST';
    case UCWORDS = 'UCWORDS';
    case UPPER = 'UPPER';
}
