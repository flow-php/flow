<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

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
