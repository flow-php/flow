<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\BulkOperation;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\BulkInsert;
use Flow\ETL\Adapter\Doctrine\BulkOperation;

final class InsertOrSkipOnConflict implements BulkOperation
{
    private BulkInsert $bulkInsert;

    public function __construct(BulkInsert $bulkInsert)
    {
        $this->bulkInsert = $bulkInsert;
    }

    public function execute(string $table, BulkData $bulkData) : void
    {
        $this->bulkInsert->insertOrSkipOnConflict($table, $bulkData);
    }
}
