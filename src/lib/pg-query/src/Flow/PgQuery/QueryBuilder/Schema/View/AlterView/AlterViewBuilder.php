<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\AlterView;

final readonly class AlterViewBuilder implements AlterViewActionStep
{
    private function __construct(
        private ?string $name = null,
        private ?string $schema = null,
        private bool $ifExists = false,
    ) {
    }

    public static function create(string $name, ?string $schema = null) : AlterViewActionStep
    {
        $parts = \explode('.', $name);

        if (\count($parts) === 2) {
            return new self($parts[1], $parts[0]);
        }

        return new self($name, $schema);
    }

    public function ifExists() : AlterViewActionStep
    {
        return new self(
            $this->name,
            $this->schema,
            true,
        );
    }

    public function ownerTo(string $owner) : AlterViewOwnerFinalStep
    {
        return AlterViewOwnerBuilder::create($this->name ?? '', $this->schema, $owner);
    }

    public function renameTo(string $newName) : RenameViewFinalStep
    {
        return RenameViewBuilder::create($this->name ?? '', $this->schema, $newName, $this->ifExists);
    }

    public function setSchema(string $schema) : AlterViewSchemaFinalStep
    {
        return AlterViewSchemaBuilder::create($this->name ?? '', $this->schema, $schema, $this->ifExists);
    }
}
