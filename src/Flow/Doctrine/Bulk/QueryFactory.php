<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;

interface QueryFactory
{
    /**
     * @param AbstractPlatform $platform
     * @param string $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function insert(AbstractPlatform $platform, string $table, BulkData $bulkData) : string;

    /**
     * @param AbstractPlatform $platform
     * @param string $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function insertOrSkipOnConflict(AbstractPlatform $platform, string $table, BulkData $bulkData) : string;

    /**
     * @param AbstractPlatform $platform
     * @param string $table
     * @param string $constraint
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function insertOrUpdateOnConstraintConflict(AbstractPlatform $platform, string $table, string $constraint, BulkData $bulkData) : string;

    /**
     * @param AbstractPlatform $platform
     * @param string $table
     * @param array<string> $conflictColumns
     * @param BulkData $bulkData
     * @param array<string> $updateColumns
     *
     * @return string
     */
    public function insertOrUpdateOnConflict(AbstractPlatform $platform, string $table, array $conflictColumns, BulkData $bulkData, array $updateColumns = []) : string;
}
