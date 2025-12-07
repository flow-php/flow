<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

enum IndexCleanup : string
{
    case AUTO = 'auto';
    case OFF = 'off';
    case ON = 'on';
}
