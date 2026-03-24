<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\Column;

/**
 * @internal
 */
final readonly class MapPlan
{
    /**
     * @param array{flatPath: string, isRequired: bool, converter: ?Converter, target: WriteFlatColumnValues} $optionalKey
     */
    public function __construct(
        public string $childName,
        public bool $isRequired,
        public FlatPlan $keyPlan,
        public FlatPlan|StructPlan|ListPlan|self|null $valuePlan,
        public array $optionalKey,
        public ?Column $valueColumn = null,
    ) {
    }
}
