<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex;

interface AlterIndexFinalStep
{
    public function ifExists() : self;

    public function renameTo(string $newName) : RenameIndexFinalStep;

    public function setTablespace(string $tablespace) : AlterTablespaceIndexFinalStep;
}
