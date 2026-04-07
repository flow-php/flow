<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Generator\MigrationGenerator;
use Flow\PostgreSql\Migrations\Version;

final class SpyMigrationGenerator implements MigrationGenerator
{
    public ?string $lastDataName = null;

    /**
     * @var ?list<string>
     */
    public ?array $lastDownSql = null;

    public ?string $lastSchemaName = null;

    /**
     * @var ?list<string>
     */
    public ?array $lastUpSql = null;

    public function __construct(
        public Version $returnVersion,
    ) {
    }

    public function generateDataMigration(?string $name = null) : Version
    {
        $this->lastDataName = $name;

        return $this->returnVersion;
    }

    public function generateSchemaMigration(?string $name, array $upSql, ?array $downSql = null) : Version
    {
        $this->lastSchemaName = $name;
        $this->lastUpSql = $upSql;
        $this->lastDownSql = $downSql;

        return $this->returnVersion;
    }
}
