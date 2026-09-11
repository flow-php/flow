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

use function count;

final class SequenceExtractor implements BatchableExtractor, Extractor, InfersSchema, RewindableExtractor
{
    use Batches;

    private ?Schema $derivedSchema = null;

    private SchemaInference $inference;

    private ?Schema $schema = null;

    public function __construct(
        private readonly SequenceGenerator $generator,
        private readonly string $entryName = 'entry',
    ) {
        $this->inference = new SchemaInference();
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $batchSize = $this->batchSize();
        $schema = $this->schema();
        $buffer = [];
        $batches = new InferredRows('the sequence', $this->schema === null ? $this->inference : null);

        /** @var mixed $item */
        foreach ($this->generator->generate() as $item) {
            $buffer[] = [$this->entryName => $item];

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
