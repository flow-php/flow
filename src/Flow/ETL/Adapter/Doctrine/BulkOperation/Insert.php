<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\BulkOperation;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\Bulk;
use Flow\Doctrine\Bulk\BulkData;
use Flow\ETL\Adapter\Doctrine\BulkOperation;

/**
 * @deprecated
 */
final class Insert implements BulkOperation
{
    private Bulk $bulk;

    public function __construct(Bulk $bulk)
    {
        $this->bulk = $bulk;
    }

    /**
     * @psalm-suppress DeprecatedClass
     */
    public function execute(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $this->bulk->insert($connection, $table, $bulkData);
    }
}
