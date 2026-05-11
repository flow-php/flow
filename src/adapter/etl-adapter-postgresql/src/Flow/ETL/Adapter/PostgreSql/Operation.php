<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

enum Operation: string
{
    case DELETE = 'delete';
    case INSERT = 'insert';
    case UPDATE = 'update';
}
