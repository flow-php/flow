<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

interface AlterRoleActionStep
{
    public function renameTo(string $newName) : AlterRoleRenameFinalStep;

    public function set() : AlterRoleFinalStep;
}
