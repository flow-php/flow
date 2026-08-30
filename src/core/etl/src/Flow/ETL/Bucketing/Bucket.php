<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;
use Flow\ETL\Schema;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final readonly class Bucket
{
    public function __construct(
        public string $id,
        public int $totalRows,
        public int $index,
    ) {}

    public static function schema(): Schema
    {
        return schema(str_schema(BucketShape::id->value), int_schema(BucketShape::totalRows->value));
    }

    public function toRow(): Row
    {
        return row([
            BucketShape::id->value => $this->id,
            BucketShape::totalRows->value => $this->totalRows,
        ]);
    }
}
