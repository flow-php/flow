<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\SequenceGenerator\SequenceGenerator;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class SequenceExtractor implements Extractor, InfersSchema
{
    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private ?Schema $schema = null;

    public function __construct(
        private readonly SequenceGenerator $generator,
        private readonly string $entryName = 'entry',
    ) {
        $this->inference = new SchemaInference(sampleSize: -1);
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        /** @var mixed $item */
        foreach ($this->generator->generate() as $item) {
            $signal = yield array_to_rows([[$this->entryName => $item]], $schema, $context->hydrator());

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

        /** @var callable(): Generator<int, array<string, mixed>> $items */
        $items = function (): Generator {
            /** @var mixed $item */
            foreach ($this->generator->generate() as $item) {
                yield [$this->entryName => $item];
            }
        };

        // No spill: generate() returns a fresh Generator per call, so extract() gets its own pass and
        // spilling a generated sequence would write gigabytes to learn "integer".
        //
        // No seeded name either: PHP re-keys a canonical-integer entry name to an int inside $items, so
        // InMemoryRows renames it through ColumnName and a raw seed would describe a second, empty column.
        return $this->derivedSchema = (new SchemaInferrer($this->inference, new InstanceOfTypeNarrower()))->infer(
            [],
            (new InMemoryRows($items()))->samples($this->inference->sampleSize),
        );
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
