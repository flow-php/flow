<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\QueryFactory;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\{BulkData, DbalPlatform, QueryFactory, TableDefinition};

final class DbalQueryFactory implements QueryFactory
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
     * @throws RuntimeException
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string
    {
        return (new DbalPlatform($platform))->dialect()->prepareInsert($table, $bulkData, $insertOptions);
    }

    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array{
     *  primary_key_columns?: array<string>,
     *  update_columns?: array<string>
     * } $updateOptions $updateOptions
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function update(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string
    {
        return (new DbalPlatform($platform))->dialect()->prepareUpdate($table, $bulkData, $updateOptions);
    }
}
