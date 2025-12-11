<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

interface GrantRoleToStep
{
    public function to(string ...$roles) : GrantRoleFinalStep;
}
