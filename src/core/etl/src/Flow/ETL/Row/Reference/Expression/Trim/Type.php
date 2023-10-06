<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression\Trim;

enum Type : string
{
    case BOTH = 'trim';

    case LEFT = 'ltrim';

    case RIGHT = 'rtrim';
}
