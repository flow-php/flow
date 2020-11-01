<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;

interface QueryFactory
{
    public function insert(AbstractPlatform $platform, string $table, BulkData $bulkData) : string;

    public function insertOrSkipOnConflict(AbstractPlatform $platform, string $table, BulkData $bulkData) : string;

    public function insertOrUpdateOnConstraintConflict(AbstractPlatform $platform, string $table, string $constraint, BulkData $bulkData) : string;
}
