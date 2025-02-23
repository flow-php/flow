<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\InsertOptions;

final readonly class SqliteInsertOptions implements InsertOptions
{
    public function __construct(
        public ?bool $skipConflicts = null,
        public array $conflictColumns = [],
        public array $updateColumns = [],
    ) {
    }

    public static function fromArray(array $options) : InsertOptions
    {
        return new self(
            $options['skip_conflicts'] ?? null,
            $options['conflict_columns'] ?? [],
            $options['update_columns'] ?? [],
        );
    }

    public static function new() : self
    {
        return new self();
    }

    /**
     * @param array<string> $conflictColumns
     */
    public function conflictColumns(array $conflictColumns) : self
    {
        return new self($this->skipConflicts, $conflictColumns, $this->updateColumns);
    }

    public function skipConflicts(bool $skip = true) : self
    {
        return new self($skip, $this->conflictColumns, $this->updateColumns);
    }

    /**
     * @param array<string> $updateColumns
     */
    public function updateColumns(array $updateColumns) : self
    {
        return new self($this->skipConflicts, $this->conflictColumns, $updateColumns);
    }
}
