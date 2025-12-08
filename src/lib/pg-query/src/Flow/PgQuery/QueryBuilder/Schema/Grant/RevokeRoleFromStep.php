<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

interface RevokeRoleFromStep
{
    public function from(string ...$roles) : RevokeRoleFinalStep;
}
