<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

enum TriggerEvent : string
{
    case DELETE = 'DELETE';
    case INSERT = 'INSERT';
    case TRUNCATE = 'TRUNCATE';
    case UPDATE = 'UPDATE';
}
