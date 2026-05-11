<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

enum TablePrivilege: string
{
    case ALL = 'all';
    case DELETE = 'delete';
    case INSERT = 'insert';
    case REFERENCES = 'references';
    case SELECT = 'select';
    case TRIGGER = 'trigger';
    case TRUNCATE = 'truncate';
    case UPDATE = 'update';
}
