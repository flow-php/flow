<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\InsertOptions;

final readonly class PostgreSQLInsertOptions implements InsertOptions
{
    public function __construct(
        public ?bool $skipConflicts = null,
        public ?string $constraint = null,
        public array $conflictColumns = [],
        public array $updateColumns = [],
    ) {
    }

    public static function fromArray(array $options) : InsertOptions
    {
        return new self(
            $options['skip_conflicts'] ?? null,
            $options['constraint'] ?? null,
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
        return new self($this->skipConflicts, $this->constraint, $conflictColumns, $this->updateColumns);
    }

    public function constraint(string $constraint) : self
    {
        return new self($this->skipConflicts, $constraint, $this->conflictColumns, $this->updateColumns);
    }

    public function skipConflicts(bool $skip = true) : self
    {
        return new self($skip, $this->constraint, $this->conflictColumns, $this->updateColumns);
    }

    /**
     * @param array<string> $updateColumns
     */
    public function updateColumns(array $updateColumns) : self
    {
        return new self($this->skipConflicts, $this->constraint, $this->conflictColumns, $updateColumns);
    }
}
