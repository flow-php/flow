<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\UpdateOptions;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final readonly class PostgreSQLUpdateOptions implements UpdateOptions
{
    /**
     * @param array<string> $primaryKeyColumns
     * @param array<string> $updateColumns
     */
    public function __construct(
        public array $primaryKeyColumns = [],
        public array $updateColumns = [],
        public ?bool $preserveExistingValues = null,
    ) {}

    public static function fromArray(array $options): UpdateOptions
    {
        $options = type_structure(optional_elements: [
            'primary_key_columns' => type_list(type_string()),
            'update_columns' => type_list(type_string()),
            'preserve_existing_values' => type_optional(type_boolean()),
        ])->assert($options);

        return new self(
            type_list(type_string())->assert($options['primary_key_columns'] ?? []),
            type_list(type_string())->assert($options['update_columns'] ?? []),
            type_optional(type_boolean())->assert($options['preserve_existing_values'] ?? null),
        );
    }

    public static function new(): UpdateOptions
    {
        return new self();
    }
}
