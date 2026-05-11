<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

enum TriggerTiming: int
{
    case AFTER = 0;
    case BEFORE = 2;
    case INSTEAD_OF = 64;
}
