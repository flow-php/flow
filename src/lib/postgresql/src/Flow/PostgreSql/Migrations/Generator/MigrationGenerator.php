<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Generator;

use Flow\PostgreSql\Migrations\Version;

interface MigrationGenerator
{
    public function generateDataMigration(?string $name = null) : Version;

    /**
     * @param list<string> $upSql
     * @param ?list<string> $downSql
     */
    public function generateSchemaMigration(?string $name, array $upSql, ?array $downSql = null) : Version;
}
