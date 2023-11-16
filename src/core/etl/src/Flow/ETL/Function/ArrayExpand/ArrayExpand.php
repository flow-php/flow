<?php declare(strict_types=1);

namespace Flow\ETL\Function\ArrayExpand;

enum ArrayExpand : string
{
    case BOTH = 'both';

    case KEYS = 'keys';

    case VALUES = 'values';
}
