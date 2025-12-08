<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

interface GrantRoleToStep
{
    public function to(string ...$roles) : GrantRoleFinalStep;
}
