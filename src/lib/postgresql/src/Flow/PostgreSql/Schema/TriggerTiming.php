<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

enum TriggerTiming : string
{
    case AFTER = 'AFTER';
    case BEFORE = 'BEFORE';
    case INSTEAD_OF = 'INSTEAD OF';
}
