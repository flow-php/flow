<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\InsertOptions;
use Flow\Doctrine\Bulk\TableDefinition;
use Flow\Doctrine\Bulk\UpdateOptions;

interface Dialect
{
    /**
     * The maximum number of bind parameters one statement may carry on this platform. A bulk statement
     * binds rows x columns parameters, so it is chunked to intdiv(this, columns) rows.
     *
     * @return int<1, max>
     */
    public function maxBindParameters(): int;

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function prepareDelete(TableDefinition $table, BulkData $bulkData): string;

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param null|InsertOptions $options
     *
     * @return string
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null): string;

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param null|UpdateOptions $options
     *
     * @return string
     */
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null): string;
}
