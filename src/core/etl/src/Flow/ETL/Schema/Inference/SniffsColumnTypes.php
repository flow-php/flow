<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Row\RawRowValues;
use Flow\Types\Type\TypeNarrower;
use Traversable;

/**
 * @extends Traversable<int, RawRowValues>
 */
interface SniffsColumnTypes extends Traversable
{
    /**
     * Must equal SchemaInferrer::sniff($names, <this unit's rows>, $rowBudget).
     *
     * @param list<string> $names - columns known before any row
     * @param int<0, max>|-1 $rowBudget - rows to observe, 0 for none, -1 for all of them
     */
    public function sniffColumnTypes(
        array $names,
        int $rowBudget,
        SchemaInference $inference,
        TypeNarrower $typer,
    ): ColumnTypes;
}
