<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

final readonly class AlterSchemaBuilder implements AlterSchemaActionStep
{
    private function __construct(
        private string $name,
    ) {}

    public static function create(string $name): AlterSchemaActionStep
    {
        return new self($name);
    }

    public function ownerTo(string $owner): AlterSchemaOwnerFinalStep
    {
        return AlterSchemaOwnerBuilder::create($this->name, $owner);
    }

    public function renameTo(string $newName): AlterSchemaRenameFinalStep
    {
        return AlterSchemaRenameBuilder::create($this->name, $newName);
    }
}
