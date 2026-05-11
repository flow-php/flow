<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

enum IndexCleanup: string
{
    case AUTO = 'auto';
    case OFF = 'off';
    case ON = 'on';
}
