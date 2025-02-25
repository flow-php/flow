<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\{BulkData, InsertOptions, TableDefinition, UpdateOptions};

interface Dialect
{
    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param null|InsertOptions $options
     *
     * @return string
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null) : string;

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param null|UpdateOptions $options
     *
     * @return string
     */
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null) : string;
}
