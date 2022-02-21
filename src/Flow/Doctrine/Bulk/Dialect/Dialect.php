<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\TableDefinition;

interface Dialect
{
    /**
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
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string;
}
