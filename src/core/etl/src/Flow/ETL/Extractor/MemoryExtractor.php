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

use function Flow\ETL\DSL\array_to_rows;

final class MemoryExtractor implements Extractor, InfersSchema
{
    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Memory $memory,
    ) {
        // -1: Memory::dump() returns an array, so it re-reads for free and an exact fold costs nothing.
        $this->inference = new SchemaInference(sampleSize: -1);
    }

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        foreach ($this->memory->dump() as $row) {
            $signal = yield array_to_rows([$row], $schema, $context->hydrator());

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    /**
     * The builder replaces this extractor's exact fold wholesale: an unset sampleSize is 20_480, not the
     * -1 the constructor chose, so ->inferSchema(infer_schema()->allStrings()) also bounds the sample.
     */
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
