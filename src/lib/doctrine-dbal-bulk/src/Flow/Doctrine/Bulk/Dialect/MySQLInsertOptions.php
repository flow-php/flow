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
            ]
        )->assert($options);

        return new self(
            $options['skip_conflicts'] ?? null,
            $options['upsert'] ?? null,
            $options['update_columns'] ?? [],
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
    public function updateColumns(array $updateColumns) : self
    {
        return new self($this->skipConflicts, $this->upsert, $updateColumns);
    }

    public function upsert(bool $upsert = true) : self
    {
        return new self($this->skipConflicts, $upsert, $this->updateColumns);
    }
}
