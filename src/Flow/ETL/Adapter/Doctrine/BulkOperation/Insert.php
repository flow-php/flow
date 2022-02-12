<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\BulkOperation;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\BulkInsert;
use Flow\ETL\Adapter\Doctrine\BulkOperation;

/**
 * @deprecated
 */
final class Insert implements BulkOperation
{
    private BulkInsert $bulkInsert;

    public function __construct(BulkInsert $bulkInsert)
    {
        $this->bulkInsert = $bulkInsert;
    }

    /**
     * @psalm-suppress DeprecatedClass
     */
    public function execute(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $this->bulkInsert->insert($connection, $table, $bulkData);
    }
}
