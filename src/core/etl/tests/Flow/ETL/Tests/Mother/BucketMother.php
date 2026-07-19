<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Bucket;

use function Flow\ETL\DSL\refs;

final class BucketMother
{
    public static function empty(string $id): Bucket
    {
        return self::withRowsCount($id, 0);
    }

    public static function withRowsCount(string $id, int $rowsCount): Bucket
    {
        return new Bucket($id, refs('id'), BucketStatsMother::with(rowsCount: $rowsCount));
    }
}
