<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel;

use Flow\Parquet\ParquetFile\Schema\Column;

/**
 * @internal
 */
final readonly class ListPlan
{
    public function __construct(
        public string $childName,
        public bool $isRequired,
        public FlatPlan|StructPlan|self|MapPlan $element,
        public ?Column $elementColumn = null,
    ) {}
}
