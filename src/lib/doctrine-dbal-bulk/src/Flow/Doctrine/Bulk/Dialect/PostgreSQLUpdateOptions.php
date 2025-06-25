<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use function Flow\Types\DSL\{type_list, type_string, type_structure};
use Flow\Doctrine\Bulk\UpdateOptions;

final readonly class PostgreSQLUpdateOptions implements UpdateOptions
{
    /**
     * @param array<string> $primaryKeyColumns
     * @param array<string> $updateColumns
     */
    public function __construct(
        public array $primaryKeyColumns = [],
        public array $updateColumns = [],
    ) {
    }

    /**
     * @param array<string, mixed> $options
     */
    public static function fromArray(array $options) : UpdateOptions
    {
        $options = type_structure(
            optional_elements: [
                'primary_key_columns' => type_list(type_string()),
                'update_columns' => type_list(type_string()),
            ]
        )->assert($options);

        return new self(
            $options['primary_key_columns'] ?? [],
            $options['update_columns'] ?? [],
        );
    }

    public static function new() : UpdateOptions
    {
        return new self();
    }
}
