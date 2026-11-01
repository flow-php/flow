<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\CorruptedRecordsRouter;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Memory\InMemoryRows;
use Flow\ETL\IO\Extractor\Memory\SpilledRows;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\IO\Loader;
use Flow\ETL\Storage\Memory\Memory;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function count;
use function is_array;

final class ArrayExtractor implements BatchableExtractor, Extractor, InfersSchema, Refreshable, CorruptedRecordsRouter
{
    private ?Loader $corruptedRecordsLoader = null;

    private BatchSize $batchSize;

    /**
     * @var iterable<array<mixed>> empty when the rows are a Memory's
     */
    private readonly iterable $dataset;

    private ?Schema $derivedSchema = null;

    private readonly Filesystem $filesystem;

    private SchemaInference $inference;

    private readonly ?Memory $memory;

    private ?Schema $schema = null;

    /**
     * NOT ?Sniffer: rows() is not on the interface.
     */
    private ?SpilledRows $source = null;

    private readonly ?Path $spillRoot;

    private ?Statistics $statistics = null;

    /**
     * @param iterable<array<mixed>>|Memory $dataset an array or a Memory is described in place, a Memory read at every
     *                                               describe and extract and described again after refresh(); anything
     *                                               else is read exactly once, spilled, and replayed
     * @param null|Path $spillRoot null resolves to $filesystem->getSystemTmpDir()
     */
    public function __construct(
        iterable|Memory $dataset,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        ?Path $spillRoot = null,
    ) {
        $this->memory = $dataset instanceof Memory ? $dataset : null;
        $this->dataset = $dataset instanceof Memory ? [] : $dataset;
        $this->filesystem = $filesystem;
        $this->spillRoot = $spillRoot;
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

    public function corruptedRecordsLoader(): ?Loader
    {
        return $this->corruptedRecordsLoader;
    }

    /**
     * @return Generator<int, CorruptedRecordException|Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        // a one-shot dataset is typed from every row it spills, so only an in-place dataset's bounded sample can be outrun
        $inPlace = $this->inPlace();
        $sample = $this->schema === null && $inPlace !== null ? $this->inference : null;
        $schema = $this->describe();
        $batches = $this->schema === null
            ? RecordBatch::inferred($schema, $backend, $this->batchSize(), null, $sample?->sampleSize, $schema)
            : new RecordBatch($schema, $backend, $this->batchSize(), known: $schema);

        try {
            foreach ($batches->of($this->source?->rows() ?? $inPlace ?? $this->dataset) as $rows) {
                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        } catch (SchemaMismatchException $mismatch) {
            throw InferredSchemaException::whenPastTheSample(
                $this->memory === null ? 'from_array()' : 'from_memory()',
                $sample?->sampleSize,
                $mismatch,
            );
        }
    }

    /**
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
        $schema = $this->describe();

        if ($this->schema === null && $schema->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom(
                $this->memory === null ? 'from_array()' : 'from_memory()',
                'no row carries a column',
            );
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

        $inPlace = $this->inPlace();

        if ($inPlace !== null) {
            return $this->derivedSchema = (new SchemaInferrer($this->inference))->infer(
                new InMemoryRows($inPlace),
            )->schema;
        }

        $this->source ??= new SpilledRows(
            $this->dataset,
            $this->filesystem,
            $this->spillRoot ?? $this->filesystem->getSystemTmpDir(),
        );

        // SpilledRows writes every row, so the inference reads every row
        return $this->derivedSchema = (new SchemaInferrer(
            (new SchemaInference(types: $this->inference->types, unionByName: $this->inference->unionByName))
                ->everyRow()
                ->everySource(),
        ))->infer($this->source)->schema;
    }

    public function statistics(): Statistics
    {
        // a generator is one-shot and spilled, so its length is unknown until it has been read
        return $this->statistics ??= new Statistics(rows: match (true) {
            $this->memory !== null => Cardinality::exact($this->memory->count()),
            is_array($this->dataset) => Cardinality::exact(count($this->dataset)),
            default => Cardinality::unknown(),
        });
    }

    /**
     * A Memory may have changed since the last plan: its inferred schema and its row count are read again. An array
     * cannot change, and a spilled one-shot dataset cannot be read again.
     */
    public function refresh(): void
    {
        if ($this->memory !== null) {
            $this->derivedSchema = null;
            $this->statistics = null;
        }
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withCorruptedRecords(?Loader $loader): static
    {
        $this->corruptedRecordsLoader = $loader;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    /**
     * @return null|array<array<mixed>> the rows read in place - an array, or a Memory's dump taken now
     */
    private function inPlace(): ?array
    {
        return $this->memory?->dump() ?? (is_array($this->dataset) ? $this->dataset : null);
    }
}
