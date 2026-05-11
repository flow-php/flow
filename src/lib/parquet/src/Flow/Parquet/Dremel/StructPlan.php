<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;

/**
 * @internal
 */
final readonly class StructPlan
{
    /**
     * @param array<FlatPlan|ListPlan|MapPlan|StructPlan> $children
     * @param array<array{flatPath: string, target: WriteFlatColumnValues}> $nullFlatChildren
     */
    public function __construct(
        public string $childName,
        public bool $isRequired,
        public array $children,
        public array $nullFlatChildren,
    ) {}
}
