<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema;
use Flow\Types\Type\TypeNarrower;

use function max;

final readonly class SchemaInferrer
{
    public function __construct(
        private SchemaInference $inference,
        private TypeNarrower $typer,
    ) {}

    /**
     * The outer iterable must yield UNSTARTED inner iterables - a source counts as opened only when it is first
     * advanced, and a source abandoned mid-way is closed by its producer, not here.
     *
     * @param list<string> $names - columns known before any row; [] when the format carries none (JSON)
     * @param iterable<int, iterable<int, RawRowValues>> $sources - one inner iterable per source, in listing order.
     *                                                             A source that lists no files yields exactly one.
     *                                                             An empty outer iterable yields every name as ?string.
     */
    public function infer(array $names, iterable $sources): Schema
    {
        $columns = new ColumnTypes($names, $this->typer);
        $opened = 0;

        foreach ($sources as $source) {
            if ($this->inference->filesToSniff !== -1 && $opened >= $this->inference->filesToSniff) {
                break;
            }

            if ($this->inference->sampleSize !== -1 && $columns->rows() >= $this->inference->sampleSize) {
                break;
            }

            $opened++;

            $columns = $columns->merge(
                $this->sniff(
                    $names,
                    $source,
                    $this->inference->sampleSize === -1 ? -1 : max(0, $this->inference->sampleSize - $columns->rows()),
                ),
                // the first source always establishes the column set - with no header there is nothing to
                // establish it from - and only later sources are held to it
                $opened === 1 || $this->inference->unionByName,
            );
        }

        return $columns->schema(new TypeFloor($this->inference->candidates()));
    }

    /**
     * Returns the PRE-FLOOR partial: partials from several sources have to merge before TypeFloor, which is lossy.
     * It opens nothing and closes nothing.
     *
     * @param list<string> $names - columns known before any row; [] when the format carries none (JSON)
     * @param iterable<int, RawRowValues> $source - one source's rows, unstarted
     * @param int<0, max>|-1 $rowBudget - rows to observe, 0 for none, -1 for all of them
     */
    public function sniff(array $names, iterable $source, int $rowBudget): ColumnTypes
    {
        $columns = new ColumnTypes($names, $this->typer);

        if ($rowBudget === 0) {
            return $columns;
        }

        foreach ($source as $row) {
            $columns->observe($row);

            if ($rowBudget !== -1 && $columns->rows() >= $rowBudget) {
                break;
            }
        }

        return $columns;
    }
}
