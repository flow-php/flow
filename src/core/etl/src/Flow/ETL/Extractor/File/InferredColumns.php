<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;

use function array_diff;
use function array_unique;
use function array_values;

/**
 * The columns an inferred schema was derived from, which every file a grid format reads must carry.
 */
final readonly class InferredColumns
{
    /**
     * @param Schema $inferred the inferred body, file constant columns excluded
     * @param list<string> $tail the file constant columns, never part of a file's own columns
     * @param string $inferredFrom the source the inferred header was read from
     */
    public function __construct(
        private Schema $inferred,
        private array $tail,
        private SchemaInference $inference,
        private string $inferredFrom,
    ) {}

    /**
     * Refuses a file whose own columns (header) are not the inferred ones; a file with no columns passes.
     *
     * @param list<string> $columns the file's columns, constant columns and duplicates allowed
     *
     * @throws InferredSchemaException
     */
    public function refuseDivergence(string $source, array $columns): void
    {
        $expected = $this->inferred->references()->names();
        $columns = array_values(array_diff(array_unique($columns), $this->tail));

        if ($columns !== [] && (array_diff($columns, $expected) !== [] || array_diff($expected, $columns) !== [])) {
            throw InferredSchemaException::columnsDiverge(
                $source,
                $this->inferredFrom,
                $this->inferred,
                $columns,
                $this->inference,
            );
        }
    }
}
