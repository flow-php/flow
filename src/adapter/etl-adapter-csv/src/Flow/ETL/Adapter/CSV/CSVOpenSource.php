<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\Types\Type\TypeNarrower;
use Generator;

interface CSVOpenSource
{
    public function close(): void;

    /**
     * This instance is consumed afterwards.
     *
     * @return list<string>
     */
    public function columns(): array;

    /**
     * This instance is consumed afterwards.
     *
     * @return Generator<int, RawRowValues>
     */
    public function records(): Generator;

    /**
     * SchemaInferrer::sniff() over records(). This instance is consumed afterwards.
     *
     * @param list<string> $names
     * @param int<0, max>|-1 $rowBudget
     */
    public function sniff(array $names, int $rowBudget, SchemaInference $inference, TypeNarrower $typer): ColumnTypes;
}
