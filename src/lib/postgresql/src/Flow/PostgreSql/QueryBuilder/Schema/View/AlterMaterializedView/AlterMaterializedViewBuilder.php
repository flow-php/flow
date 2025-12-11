<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

final readonly class AlterMaterializedViewBuilder implements AlterMatViewActionStep
{
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $ifExists = false,
    ) {
    }

    public static function create(string $name, ?string $schema = null) : AlterMatViewActionStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $schema ?? $identifier->schema());
    }

    public function ifExists() : AlterMatViewActionStep
    {
        return new self(
            $this->name,
            $this->schema,
            true,
        );
    }

    public function ownerTo(string $owner) : AlterMatViewOwnerFinalStep
    {
        return AlterMatViewOwnerBuilder::create($this->name ?? '', $this->schema, $owner);
    }

    public function renameTo(string $newName) : RenameMatViewFinalStep
    {
        return RenameMatViewBuilder::create($this->name ?? '', $this->schema, $newName, $this->ifExists);
    }

    public function setSchema(string $schema) : AlterMatViewSchemaFinalStep
    {
        return AlterMatViewSchemaBuilder::create($this->name ?? '', $this->schema, $schema, $this->ifExists);
    }

    public function setTablespace(string $tablespace) : AlterMatViewTablespaceFinalStep
    {
        return AlterMatViewTablespaceBuilder::create($this->name ?? '', $this->schema, $tablespace, $this->ifExists);
    }
}
