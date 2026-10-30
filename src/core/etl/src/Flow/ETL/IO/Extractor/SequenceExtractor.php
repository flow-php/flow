<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Memory\InMemoryRows;
use Flow\ETL\IO\Extractor\SequenceGenerator\SequenceGenerator;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;
use Flow\ETL\Values\Schema;
use Generator;

final class SequenceExtractor implements BatchableExtractor, Extractor, InfersSchema
{
    private BatchSize $batchSize;

    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    public function __construct(
        private readonly SequenceGenerator $generator,
        private readonly string $columnName,
    ) {
        $this->inference = new SchemaInference();
        $this->batchSize = new BatchSize();
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize->size;
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $sample = $this->schema === null ? $this->inference : null;
        $batches = new RecordBatch($this->describe(), $backend, $this->batchSize());

        try {
            /** @var mixed $item */
            foreach ($this->generator->generate() as $item) {
                $rows = $batches->add([$this->columnName => $item]);

                if ($rows === null) {
                    continue;
                }

                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }

            $rest = $batches->flush();

            if ($rest !== null) {
                yield $rest;
            }
        } catch (SchemaMismatchException $mismatch) {
            throw InferredSchemaException::whenPastTheSample('the sequence', $sample?->sampleSize, $mismatch);
        }
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->derivedSchema = null;

        return $this;
    }

    public function schema(): Schema
    {
        $schema = $this->describe();

        if ($this->schema === null && $schema->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom('the sequence', 'it generates no item');
        }

        return $schema;
    }

    private function describe(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        if ($this->derivedSchema !== null) {
            return $this->derivedSchema;
        }

        /** @var callable(): Generator<int, array<string, mixed>> $items */
        $items = function (): Generator {
            /** @var mixed $item */
            foreach ($this->generator->generate() as $item) {
                yield [$this->columnName => $item];
            }
        };

        // No spill: generate() returns a fresh Generator per call, so extract() gets its own pass and
        // spilling a generated sequence would write gigabytes to learn "integer".
        //
        // No seeded name either: PHP re-keys a canonical-integer column name to an int inside $items, so
        // InMemoryRows renames it through ColumnName and a raw seed would describe a second, empty column.
        return $this->derivedSchema = (new SchemaInferrer($this->inference))->infer(new InMemoryRows($items()))->schema;
    }

    public function statistics(): Statistics
    {
        return $this->statistics ??= new Statistics(rows: $this->generator->rows());
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
