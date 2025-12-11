<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

enum FunctionVolatility : string
{
    case IMMUTABLE = 'immutable';
    case STABLE = 'stable';
    case VOLATILE = 'volatile';
}
