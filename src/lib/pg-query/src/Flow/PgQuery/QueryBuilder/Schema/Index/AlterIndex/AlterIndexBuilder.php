<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex;

final readonly class AlterIndexBuilder implements AlterIndexFinalStep
{
    private function __construct(
        private string $index,
        private ?string $schema = null,
        private bool $ifExists = false,
    ) {
    }

    public static function create(string $index, ?string $schema = null) : AlterIndexFinalStep
    {
        return new self($index, $schema);
    }

    public function ifExists() : AlterIndexFinalStep
    {
        return new self(
            $this->index,
            $this->schema,
            true,
        );
    }

    public function renameTo(string $newName) : RenameIndexFinalStep
    {
        return RenameIndexBuilder::create($this->index, $this->schema, $newName, $this->ifExists);
    }

    public function setTablespace(string $tablespace) : AlterTablespaceIndexFinalStep
    {
        return AlterTablespaceIndexBuilder::create($this->index, $this->schema, $tablespace, $this->ifExists);
    }
}
