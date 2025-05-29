<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\QueryFactory;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\{BulkData, DbalPlatform, InsertOptions, QueryFactory, TableDefinition, UpdateOptions};
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class DbalQueryFactory implements QueryFactory
{
    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function delete(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData) : string
    {
        return (new DbalPlatform($platform))->dialect()->prepareDelete($table, $bulkData);
    }

    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param ?InsertOptions $options
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null) : string
    {
        return (new DbalPlatform($platform))->dialect()->prepareInsert($table, $bulkData, $options);
    }

    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param null|UpdateOptions $options
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function update(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null) : string
    {
        return (new DbalPlatform($platform))->dialect()->prepareUpdate($table, $bulkData, $options);
    }
}
