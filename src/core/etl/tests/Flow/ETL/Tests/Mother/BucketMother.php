<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Bucket;

final class BucketMother
{
    public static function empty(string $id, int $index = 0): Bucket
    {
        return new Bucket($id, 0, $index);
    }

    public static function withTotalRows(string $id, int $totalRows, int $index = 0): Bucket
    {
        return new Bucket($id, $totalRows, $index);
    }
}
