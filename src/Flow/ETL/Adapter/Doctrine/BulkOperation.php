<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Flow\Doctrine\Bulk\BulkData;

/**
 * @deprecated
 */
interface BulkOperation
{
    public function execute(Connection $connection, string $table, BulkData $bulkData) : void;
}
