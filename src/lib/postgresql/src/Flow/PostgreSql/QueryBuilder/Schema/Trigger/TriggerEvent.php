<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

enum TriggerEvent : int
{
    case DELETE = 8;
    case INSERT = 4;
    case TRUNCATE = 32;
    case UPDATE = 16;
}
