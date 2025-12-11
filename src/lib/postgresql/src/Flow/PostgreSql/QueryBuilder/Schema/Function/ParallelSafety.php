<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

enum ParallelSafety : string
{
    case RESTRICTED = 'restricted';
    case SAFE = 'safe';
    case UNSAFE = 'unsafe';
}
