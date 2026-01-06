<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use function Flow\Types\DSL\{type_boolean, type_list, type_optional, type_string, type_structure};
use Flow\Doctrine\Bulk\InsertOptions;

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
    ) {
    }

    public static function fromArray(array $options) : InsertOptions
    {
        $options = type_structure(
            [],
            [
                'skip_conflicts' => type_optional(type_boolean()),
                'upsert' => type_optional(type_boolean()),
                'update_columns' => type_list(type_string()),
                'preserve_existing_values' => type_optional(type_boolean()),
            ]
        )->assert($options);

        return new self(
            $options['skip_conflicts'] ?? null,
            $options['upsert'] ?? null,
            $options['update_columns'] ?? [],
            $options['preserve_existing_values'] ?? null,
        );
    }

    public static function new() : self
    {
        return new self();
    }

    public function skipConflicts(bool $skip = true) : self
    {
        return new self($skip, $this->upsert, $this->updateColumns);
    }

    /**
     * @param array<string> $updateColumns
     */
    public function updateColumns(array $updateColumns, ?bool $preserveExistingValues = null) : self
    {
        return new self($this->skipConflicts, $this->upsert, $updateColumns, $preserveExistingValues);
    }

    public function upsert(bool $upsert = true) : self
    {
        return new self($this->skipConflicts, $upsert, $this->updateColumns);
    }
}
