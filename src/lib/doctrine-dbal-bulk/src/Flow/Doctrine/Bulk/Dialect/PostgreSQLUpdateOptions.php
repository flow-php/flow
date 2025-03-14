<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\UpdateOptions;

final readonly class PostgreSQLUpdateOptions implements UpdateOptions
{
    public function __construct(
        public array $primaryKeyColumns = [],
        public array $updateColumns = [],
    ) {
    }

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
