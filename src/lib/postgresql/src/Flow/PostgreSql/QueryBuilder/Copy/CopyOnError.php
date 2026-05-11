<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Copy;

enum CopyOnError: string
{
    case IGNORE = 'ignore';
    case STOP = 'stop';
}
