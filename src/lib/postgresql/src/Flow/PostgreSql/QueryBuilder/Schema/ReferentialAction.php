<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema;

enum ReferentialAction: string
{
    case CASCADE = 'c';
    case NO_ACTION = 'a';
    case RESTRICT = 'r';
    case SET_DEFAULT = 'd';
    case SET_NULL = 'n';
}
