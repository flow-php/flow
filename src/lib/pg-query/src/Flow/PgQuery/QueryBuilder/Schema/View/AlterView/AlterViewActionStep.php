<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterView;

interface AlterViewActionStep
{
    public function ifExists() : self;

    public function ownerTo(string $owner) : AlterViewOwnerFinalStep;

    public function renameTo(string $newName) : RenameViewFinalStep;

    public function setSchema(string $schema) : AlterViewSchemaFinalStep;
}
