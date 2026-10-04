<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\Memory;

use Flow\ETL\Row\ColumnName;
use Flow\ETL\Schema\Inference\SchemaSampler;
use Generator;

final readonly class InMemoryRows implements SchemaSampler
{
    /**
     * @param iterable<array<mixed>> $rows
     */
    public function __construct(
        private iterable $rows,
    ) {}

    /**
     * One inner iterable, because the sampled unit is one SOURCE - the same choice a CSV reader makes
     * when it yields one iterable per listed file rather than per byte range.
     *
     * $rowBudget is never rationed: the inner generator is lazy and SchemaInferrer stops advancing it
     * when the budget is spent.
     *
     * @return iterable<int, iterable<int, array<array-key, mixed>>>
     */
    public function samples(int $rowBudget): iterable
    {
        // A plain array, not `yield`: a body containing `yield` would make samples() itself a
        // generator. Harmless here, but it must match SpilledRows, where it is load-bearing.
        return [$this->values()];
    }

    /**
     * The same ColumnName normalisation array_to_rows() applies, so the fold's names are the
     * batch builder's names.
     *
     * @return Generator<int, array<array-key, mixed>>
     */
    public function values(): Generator
    {
        $columnName = new ColumnName();

        foreach ($this->rows as $row) {
            $values = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $key => $value) {
                $values[$columnName->of($key)] = $value;
            }

            yield $values;
        }
    }
}
