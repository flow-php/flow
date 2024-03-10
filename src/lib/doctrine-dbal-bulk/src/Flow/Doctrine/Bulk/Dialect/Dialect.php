<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\{BulkData, TableDefinition};

interface Dialect
{
    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array<mixed> $insertOptions
     *
     * @return string
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string;

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array<mixed> $updateOptions
     *
     * @return string
     */
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string;
}
