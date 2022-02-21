<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\QueryFactory;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\DbalPlatform;
use Flow\Doctrine\Bulk\QueryFactory;
use Flow\Doctrine\Bulk\TableDefinition;

class DbalQueryFactory implements QueryFactory
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
     *@throws \Flow\Doctrine\Bulk\Exception\RuntimeException
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string
    {
        return (new DbalPlatform($platform))->dialect()->prepareInsert($table, $bulkData, $insertOptions);
    }
}
