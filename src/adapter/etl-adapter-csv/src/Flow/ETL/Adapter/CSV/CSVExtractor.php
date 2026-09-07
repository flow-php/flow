<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\InfersSchema;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Generator;

use function array_diff;
use function array_keys;
use function array_values;
use function iterator_to_array;
use function sprintf;

final class CSVExtractor implements Extractor, FileExtractor, InfersSchema, LimitableExtractor, MetadataColumnsExtractor
{
    use Limitable;
    use FileReading;

    private SchemaInference $inference;

    private CSVReadOptions $readOptions;

    private ?Schema $schema = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_csv($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->inference = new SchemaInference();
        $this->readOptions = new CSVReadOptions();
        $this->resetLimit();
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $sources = iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false);
        $reader = new CSVFileReader(new CSVSourceOpener($this->filesystem, $this->readOptions), $sources);

        if ($this->schema !== null) {
            $base = $this->schema;
        } else {
            // a local, not the property: withoutTail() takes a non-nullable Schema and no analyzer narrows a property
            $derived = $this->derivedSchema;

            if ($derived === null) {
                $derived =
                    $this->derivedSchema = (new SchemaInferrer(
                        $this->inference,
                        new StringTypeNarrower($this->inference->candidates()->toArray()),
                    ))->infer($reader->header()->names, $reader->samples($this->inference->sampleSize));
            }

            $base = $fileColumns->withoutTail($derived);
        }

        $schema = $fileColumns->declare($base);
        $tail = $fileColumns->tail();
        $expected = $base->references()->names();

        foreach ($sources as $source) {
            // forFile() reads the PARTITION definitions, which only declare() creates - $base is the body
            $constants = $fileColumns->forFile($source, $schema);
            $columns = null;

            foreach ($reader->batches($source, $batchSize) as $rawBatch) {
                if ($columns === null) {
                    $columns = array_values(array_diff(array_keys($rawBatch[0]->values), $tail));

                    if (
                        $this->schema === null
                        && !$this->inference->unionByName
                        && (array_diff($columns, $expected) !== [] || array_diff($expected, $columns) !== [])
                    ) {
                        throw InferredSchemaException::columnsDiverge(
                            $source->uri(),
                            $reader->header()->source ?? '',
                            $base,
                            $columns,
                            $this->inference,
                        );
                    }
                }

                $batch = [];

                foreach ($rawBatch as $values) {
                    $batch[] = new RawRowValues($constants->fill($values->values));
                }

                $hydrated = $hydrator->hydrate($batch, $schema);

                foreach ($hydrated as $hydratedRow) {
                    $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                    $this->incrementReturnedRows();

                    if ($signal === Signal::STOP || $this->reachedLimit()) {
                        return;
                    }
                }
            }

            if ($columns === null && $this->schema === null && !$this->inference->unionByName) {
                $columns = array_values(array_diff($reader->columns($source), $tail));

                if (
                    $columns !== []
                    && (array_diff($columns, $expected) !== [] || array_diff($expected, $columns) !== [])
                ) {
                    throw InferredSchemaException::columnsDiverge(
                        $source->uri(),
                        $reader->header()->source ?? '',
                        $base,
                        $columns,
                        $this->inference,
                    );
                }
            }
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
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);

        if ($this->schema !== null) {
            return $fileColumns->declare($this->schema);
        }

        // a local, not the property: withoutTail() takes a non-nullable Schema and no analyzer narrows a property
        $derived = $this->derivedSchema;

        if ($derived === null) {
            $reader = new CSVFileReader(
                new CSVSourceOpener($this->filesystem, $this->readOptions),
                iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false),
            );

            $derived =
                $this->derivedSchema = (new SchemaInferrer(
                    $this->inference,
                    new StringTypeNarrower($this->inference->candidates()->toArray()),
                ))->infer($reader->header()->names, $reader->samples($this->inference->sampleSize));
        }

        return $fileColumns->declare($fileColumns->withoutTail($derived));
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withBOMRemoval(bool $removeBOM): self
    {
        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withRemoveBOM($removeBOM);

        return $this;
    }

    public function withCharactersReadInLine(int $charactersReadInLine): self
    {
        if ($charactersReadInLine < 1) {
            throw new InvalidArgumentException('Characters read in line must be greater than 0');
        }

        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withCharactersReadInLine($charactersReadInLine);

        return $this;
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withEmptyToNull($emptyToNull);

        return $this;
    }

    public function withEnclosure(string $enclosure): self
    {
        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withEnclosure($enclosure);

        return $this;
    }

    public function withEscape(string $escape): self
    {
        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withEscape($escape);

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withHeader($withHeader);

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSeparator(string $separator): self
    {
        $this->derivedSchema = null;
        $this->readOptions = $this->readOptions->withSeparator($separator);

        return $this;
    }
}
