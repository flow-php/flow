<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use function Flow\Types\DSL\{type_boolean, type_list, type_optional, type_string, type_structure};
use Flow\Doctrine\Bulk\InsertOptions;

final readonly class PostgreSQLInsertOptions implements InsertOptions
{
    /**
     * @param array<string> $conflictColumns
     * @param array<string> $updateColumns
     */
    public function __construct(
        public ?bool $skipConflicts = null,
        public ?string $constraint = null,
        public array $conflictColumns = [],
        public array $updateColumns = [],
    ) {
    }

    /**
     * @param array<string, mixed> $options
     */
    public static function fromArray(array $options) : InsertOptions
    {
        $options = type_structure(
            [],
            [
                'skip_conflicts' => type_optional(type_boolean()),
                'constraint' => type_optional(type_string()),
                'conflict_columns' => type_list(type_string()),
                'update_columns' => type_list(type_string()),
            ]
        )->assert($options);

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
