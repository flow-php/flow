<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

enum IndexMethod: string
{
    case BRIN = 'brin';
    case BTREE = 'btree';
    case GIN = 'gin';
    case GIST = 'gist';
    case HASH = 'hash';
    case SPGIST = 'spgist';
}
