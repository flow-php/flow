<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Generator;

use function count;

final class MemoryExtractor implements BatchableExtractor, Extractor, InfersSchema, RewindableExtractor
{
    use Batches;

    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Memory $memory,
    ) {
        $this->inference = new SchemaInference();
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $batchSize = $this->batchSize();
        $schema = $this->schema();
        $buffer = [];
        $batches = new InferredRows('from_memory()', $this->schema === null ? $this->inference : null);

        foreach ($this->memory->dump() as $row) {
            $buffer[] = $row;

            if (count($buffer) < $batchSize) {
                continue;
            }

            $signal = yield $batches->of($buffer, $schema, $context->hydrator());

            if ($signal === Signal::STOP) {
                return;
            }

            $buffer = [];
        }

        if ($buffer !== []) {
            yield $batches->of($buffer, $schema, $context->hydrator());
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
        if ($this->schema !== null) {
            return $this->schema;
        }

        if ($this->derivedSchema !== null) {
            return $this->derivedSchema;
        }

        $derived = (new SchemaInferrer($this->inference, new InstanceOfTypeNarrower()))->infer(
            [],
            (new InMemoryRows($this->memory->dump()))->samples($this->inference->sampleSize),
        );

        // An empty fold says "no column was observed", not "this source has no columns" - and unlike the
        // other two sources a Memory is mutable (ArrayMemory::save() appends), so it is not an answer
        // yet and is not memoised.
        if ($derived->definitions() !== []) {
            $this->derivedSchema = $derived;
        }

        return $derived;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
