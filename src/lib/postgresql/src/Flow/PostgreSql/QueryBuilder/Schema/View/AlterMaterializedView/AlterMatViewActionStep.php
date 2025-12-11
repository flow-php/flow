<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

interface AlterMatViewActionStep
{
    public function ifExists() : self;

    public function ownerTo(string $owner) : AlterMatViewOwnerFinalStep;

    public function renameTo(string $newName) : RenameMatViewFinalStep;

    public function setSchema(string $schema) : AlterMatViewSchemaFinalStep;

    public function setTablespace(string $tablespace) : AlterMatViewTablespaceFinalStep;
}
