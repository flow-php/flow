<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function is_array;

final class ArrayExtractor implements Extractor
{
    private ?Schema $derivedSchema = null;

    private readonly Filesystem $filesystem;

    private SchemaInference $inference;

    private ?Schema $schema = null;

    /**
     * NOT ?SchemaSampler: rows() is not on the interface.
     */
    private ?SpilledRows $source = null;

    private readonly ?Path $spillRoot;

    /**
     * @param iterable<array<mixed>> $dataset an array is described in place; anything else is read
     *                                        exactly once, spilled, and replayed
     * @param null|Path $spillRoot null resolves to $filesystem->getSystemTmpDir()
     */
    public function __construct(
        private readonly iterable $dataset,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ?Path $spillRoot = null,
    ) {
        $this->filesystem = $filesystem;
        $this->spillRoot = $spillRoot;
        $this->inference = new SchemaInference(sampleSize: -1);
    }

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $schema = $this->schema();

        $rows = $this->source?->rows() ?? $this->dataset;

        foreach ($rows as $row) {
            $signal = yield array_to_rows([$row], $schema, $context->hydrator());

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    /**
     * The builder replaces this extractor's exact fold wholesale: an unset sampleSize is 20_480, not the
     * -1 the constructor chose, so ->inferSchema(infer_schema()->allStrings()) also bounds the sample.
     *
     * @throws InvalidLogicException when a one-shot dataset was already read
     */
    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        if ($this->source !== null) {
            throw InvalidLogicException::because(
                '%s cannot change its inference options after its dataset was consumed: the source was '
                . 'already read and spilled. Call inferSchema() before schema() or extract().',
                self::class,
            );
        }

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

        if (is_array($this->dataset)) {
            return $this->derivedSchema = (new SchemaInferrer($this->inference, new InstanceOfTypeNarrower()))->infer(
                [],
                (new InMemoryRows($this->dataset))->samples($this->inference->sampleSize),
            );
        }

        // ??=, not =: a schema() that threw left the sampler holding a half-read source, and rebuilding
        // it here would read that source a second time - silently returning 0 columns for a shape whose
        // exhaustion is quiet, such as NoRewindIterator or a database cursor. Reusing it lets the
        // sampler's own state machine refuse.
        $this->source ??= new SpilledRows(
            $this->dataset,
            $this->filesystem,
            $this->spillRoot ?? $this->filesystem->getSystemTmpDir(),
        );

        // The budget is forced to -1 because SpilledRows writes every row regardless, so stopping the
        // typing early would buy only the cheap half and pay for it with a guess no error can name. The
        // candidate set is carried over - it is a floor, not a bound.
        return $this->derivedSchema = (new SchemaInferrer(
            new SchemaInference(
                sampleSize: -1,
                filesToSniff: -1,
                types: $this->inference->types,
                unionByName: $this->inference->unionByName,
            ),
            new InstanceOfTypeNarrower(),
        ))->infer([], $this->source->samples(-1));
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
