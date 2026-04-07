<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

enum PartitionStrategy : string
{
    case HASH = 'hash';
    case LIST = 'list';
    case RANGE = 'range';
}
