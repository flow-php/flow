<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression\ArrayExpand;

enum ArrayExpand : string
{
    case BOTH = 'both';

    case KEYS = 'keys';

    case VALUES = 'values';
}
