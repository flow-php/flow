<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

enum IsolationLevel : string
{
    case READ_COMMITTED = 'read committed';
    case READ_UNCOMMITTED = 'read uncommitted';
    case REPEATABLE_READ = 'repeatable read';
    case SERIALIZABLE = 'serializable';
}
