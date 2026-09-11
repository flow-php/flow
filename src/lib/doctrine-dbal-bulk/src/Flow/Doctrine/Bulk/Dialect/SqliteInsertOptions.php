<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\InsertOptions;

use function Flow\Types\DSL\structure_element;
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

    public static function fromArray(array $options): InsertOptions
    {
        $options = type_structure([
            'skip_conflicts' => structure_element('skip_conflicts', type_optional(type_boolean()), optional: true),
            'conflict_columns' => structure_element('conflict_columns', type_list(type_string()), optional: true),
            'update_columns' => structure_element('update_columns', type_list(type_string()), optional: true),
            'preserve_existing_values' => structure_element(
                'preserve_existing_values',
                type_optional(type_boolean()),
                optional: true,
            ),
        ])->assert($options);

        return new self(
            type_optional(type_boolean())->assert($options['skip_conflicts'] ?? null),
            type_list(type_string())->assert($options['conflict_columns'] ?? []),
            type_list(type_string())->assert($options['update_columns'] ?? []),
            type_optional(type_boolean())->assert($options['preserve_existing_values'] ?? null),
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
