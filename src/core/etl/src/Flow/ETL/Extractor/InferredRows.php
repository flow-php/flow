<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;

use function count;
use function Flow\ETL\DSL\array_to_rows;

/**
 * The gate places a refused row in its batch; only the source knows where that batch started, so a row past a
 * bounded sample is reported here at its position in the whole source, with the way out.
 */
final class InferredRows
{
    private int $offset = 0;

    /**
     * @param null|SchemaInference $sample the inference the schema came from - null when the schema was declared
     */
    public function __construct(
        private readonly string $source,
        private readonly ?SchemaInference $sample,
    ) {}

    /**
     * @param array<array<mixed>> $rows
     *
     * @throws InferredSchemaException|SchemaMismatchException
     */
    public function of(array $rows, Schema $schema, Hydrator $hydrator): Rows
    {
        try {
            $batch = array_to_rows($rows, $schema, $hydrator);
        } catch (SchemaMismatchException $mismatch) {
            $row = $this->offset + $mismatch->rowIndex;

            if ($this->sample === null || $this->sample->sampleSize === -1 || $row < $this->sample->sampleSize) {
                throw $mismatch;
            }

            throw InferredSchemaException::pastTheSample($this->source, $row, $this->sample, $mismatch);
        }

        $this->offset += count($rows);

        return $batch;
    }
}
