<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Bucket;

final class BucketMother
{
    public static function empty(string $id): Bucket
    {
        return new Bucket($id, 0);
    }

    public static function withTotalRows(string $id, int $totalRows): Bucket
    {
        return new Bucket($id, $totalRows);
    }
}
