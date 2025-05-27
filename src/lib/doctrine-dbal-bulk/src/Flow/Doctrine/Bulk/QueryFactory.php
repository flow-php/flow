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
     *
     * @return string
     */
    public function delete(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData) : string;

    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null) : string;

    /**
     * @param AbstractPlatform $platform
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function update(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null) : string;
}
