<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

enum LockMode : int
{
    case ACCESS_EXCLUSIVE = 8;
    case ACCESS_SHARE = 1;
    case EXCLUSIVE = 7;
    case ROW_EXCLUSIVE = 3;
    case ROW_SHARE = 2;
    case SHARE = 5;
    case SHARE_ROW_EXCLUSIVE = 6;
    case SHARE_UPDATE_EXCLUSIVE = 4;
}
