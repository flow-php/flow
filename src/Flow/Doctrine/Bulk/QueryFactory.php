<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;

interface QueryFactory
{
    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions $insertOptions
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string;

    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array{
     *  primary_key_columns?: array<string>,
     *  update_columns?: array<string>
     * } $updateOptions $updateOptions
     *
     * @return string
     */
    public function update(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string;
}
