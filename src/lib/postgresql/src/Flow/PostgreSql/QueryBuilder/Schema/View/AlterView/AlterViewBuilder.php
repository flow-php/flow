<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\AlterView;

use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

final readonly class AlterViewBuilder implements AlterViewActionStep
{
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $ifExists = false,
    ) {}

    public static function create(string $name, ?string $schema = null): AlterViewActionStep
    {
        if ($schema !== null) {
            return new self($name, $schema);
        }

        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function ifExists(): AlterViewActionStep
    {
        return new self($this->name, $this->schema, true);
    }

    public function ownerTo(string $owner): AlterViewOwnerFinalStep
    {
        return AlterViewOwnerBuilder::create($this->name ?? '', $this->schema, $owner);
    }

    public function renameTo(string $newName): RenameViewFinalStep
    {
        return RenameViewBuilder::create($this->name ?? '', $this->schema, $newName, $this->ifExists);
    }

    public function setSchema(string $schema): AlterViewSchemaFinalStep
    {
        return AlterViewSchemaBuilder::create($this->name ?? '', $this->schema, $schema, $this->ifExists);
    }
}
