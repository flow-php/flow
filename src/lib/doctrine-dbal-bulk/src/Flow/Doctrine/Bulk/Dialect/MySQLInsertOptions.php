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

final readonly class MySQLInsertOptions implements InsertOptions
{
    /**
     * @param null|bool $skipConflicts
     * @param null|bool $upsert
     * @param array<string> $updateColumns
     */
    public function __construct(
        public ?bool $skipConflicts = null,
        public ?bool $upsert = null,
        public array $updateColumns = [],
        public ?bool $preserveExistingValues = null,
    ) {}

    public static function fromArray(array $options): InsertOptions
    {
        $options = type_structure([
            'skip_conflicts' => structure_element('skip_conflicts', type_optional(type_boolean()), optional: true),
            'upsert' => structure_element('upsert', type_optional(type_boolean()), optional: true),
            'update_columns' => structure_element('update_columns', type_list(type_string()), optional: true),
            'preserve_existing_values' => structure_element(
                'preserve_existing_values',
                type_optional(type_boolean()),
                optional: true,
            ),
        ])->assert($options);

        return new self(
            type_optional(type_boolean())->assert($options['skip_conflicts'] ?? null),
            type_optional(type_boolean())->assert($options['upsert'] ?? null),
            type_list(type_string())->assert($options['update_columns'] ?? []),
            type_optional(type_boolean())->assert($options['preserve_existing_values'] ?? null),
        );
    }

    public static function new(): self
    {
        return new self();
    }

    public function skipConflicts(bool $skip = true): self
    {
        return new self($skip, $this->upsert, $this->updateColumns);
    }

    /**
     * @param array<string> $updateColumns
     */
    public function updateColumns(array $updateColumns, ?bool $preserveExistingValues = null): self
    {
        return new self($this->skipConflicts, $this->upsert, $updateColumns, $preserveExistingValues);
    }

    public function upsert(bool $upsert = true): self
    {
        return new self($this->skipConflicts, $upsert, $this->updateColumns);
    }
}
