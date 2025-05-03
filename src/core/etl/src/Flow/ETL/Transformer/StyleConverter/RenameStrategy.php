<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\StyleConverter;

enum RenameStrategy : string
{
    case LOWER = 'LOWER';
    case TRANSLITERATE = 'TRANSLITERATE';
    case UCFIRST = 'UCFIRST';
    case UCWORDS = 'UCWORDS';
    case UPPER = 'UPPER';
}
