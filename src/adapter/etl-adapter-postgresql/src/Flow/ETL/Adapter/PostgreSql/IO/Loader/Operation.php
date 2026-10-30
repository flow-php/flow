<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\IO\Loader;

enum Operation: string
{
    case DELETE = 'delete';
    case INSERT = 'insert';
    case UPDATE = 'update';
}
