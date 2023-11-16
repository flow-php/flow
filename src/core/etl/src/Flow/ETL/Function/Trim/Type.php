<?php declare(strict_types=1);

namespace Flow\ETL\Function\Trim;

enum Type : string
{
    case BOTH = 'trim';

    case LEFT = 'ltrim';

    case RIGHT = 'rtrim';
}
