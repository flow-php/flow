<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

enum RuleAction: string
{
    case ALSO = 'ALSO';
    case INSTEAD = 'INSTEAD';
}
