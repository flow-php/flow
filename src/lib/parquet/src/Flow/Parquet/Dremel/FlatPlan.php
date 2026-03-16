<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Data\Converter;

/**
 * @internal
 */
final readonly class FlatPlan
{
    public function __construct(
        public string $flatPath,
        public string $childName,
        public bool $isRequired,
        public ?Converter $converter,
        public WriteFlatColumnValues $target,
    ) {
    }
}
