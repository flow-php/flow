<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

interface AlterSchemaActionStep
{
    public function ownerTo(string $owner) : AlterSchemaOwnerFinalStep;

    public function renameTo(string $newName) : AlterSchemaRenameFinalStep;
}
