<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\InsertOptions;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final readonly class SqliteInsertOptions implements InsertOptions
{
    /**
     * @param array<string> $conflictColumns
     * @param array<string> $updateColumns
     */
    public function __construct(
        public ?bool $skipConflicts = null,
        public array $conflictColumns = [],
        public array $updateColumns = [],
        public ?bool $preserveExistingValues = null,
    ) {}

    /**
     * @param array<string, mixed> $options
     */
    public static function fromArray(array $options): InsertOptions
    {
        $options = type_structure([], [
            'skip_conflicts' => type_optional(type_boolean()),
            'conflict_columns' => type_list(type_string()),
            'update_columns' => type_list(type_string()),
            'preserve_existing_values' => type_optional(type_boolean()),
        ])->assert($options);

        return new self(
            $options['skip_conflicts'] ?? null,
            $options['conflict_columns'] ?? [],
            $options['update_columns'] ?? [],
            $options['preserve_existing_values'] ?? null,
        );
    }

    public static function new(): self
    {
        return new self();
    }

    /**
     * @param array<string> $conflictColumns
     */
    public function conflictColumns(array $conflictColumns): self
    {
        return new self($this->skipConflicts, $conflictColumns, $this->updateColumns);
    }

    public function skipConflicts(bool $skip = true): self
    {
        return new self($skip, $this->conflictColumns, $this->updateColumns);
    }

    /**
     * @param array<string> $updateColumns
     */
    public function updateColumns(array $updateColumns, ?bool $preserveExistingValues = null): self
    {
        return new self($this->skipConflicts, $this->conflictColumns, $updateColumns, $preserveExistingValues);
    }
}
