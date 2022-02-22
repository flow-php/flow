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
final class InsertOrUpdateOnConstraintConflict implements BulkOperation
{
    private Bulk $bulk;

    private string $constraint;

    public function __construct(Bulk $bulk, string $constraint)
    {
        $this->bulk = $bulk;
        $this->constraint = $constraint;
    }

    /**
     * @psalm-suppress DeprecatedClass
     */
    public function execute(Connection $connection, string $table, BulkData $bulkData) : void
    {
        $this->bulk->insertOrUpdateOnConstraintConflict($connection, $table, $this->constraint, $bulkData);
    }
}
