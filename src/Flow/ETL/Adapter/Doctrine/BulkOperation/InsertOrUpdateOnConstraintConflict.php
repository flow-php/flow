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
final class InsertOrUpdateOnConstraintConflict implements BulkOperation
{
    private BulkInsert $bulkInsert;

    private string $constraint;

    public function __construct(BulkInsert $bulkInsert, string $constraint)
    {
        $this->bulkInsert = $bulkInsert;
        $this->constraint = $constraint;
    }

    /**
     * @psalm-suppress DeprecatedClass
     */
    public function execute(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $this->bulkInsert->insertOrUpdateOnConstraintConflict($connection, $table, $this->constraint, $bulkData);
    }
}
