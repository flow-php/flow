<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

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
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Generator;

use function iterator_to_array;
use function sprintf;

final class JsonExtractor implements
    Extractor,
    FileExtractor,
    InfersSchema,
    LimitableExtractor,
    MetadataColumnsExtractor
{
    use Limitable;
    use FileReading;

    private SchemaInference $inference;

    private ?string $pointer = null;

    private bool $pointerToEntryName = false;

    private ?Schema $schema = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_json($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->inference = new SchemaInference();
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
        $reader = new JsonFileReader(
            $this->filesystem,
            JsonFormat::Document,
            $this->pointer,
            $this->pointerToEntryName,
            $sources,
        );

        if ($this->schema !== null) {
            $base = $this->schema;
        } else {
            // a local, not the property: withoutTail() takes a non-nullable Schema and no analyzer narrows a property
            $derived = $this->derivedSchema;

            if ($derived === null) {
                $derived =
                    $this->derivedSchema = (new SchemaInferrer($this->inference, new InstanceOfTypeNarrower()))->infer(
                        [],
                        $reader->samples($this->inference->sampleSize),
                    );
            }

            $base = $fileColumns->withoutTail($derived);
        }

        $schema = $fileColumns->declare($base);

        foreach ($sources as $source) {
            // forFile() reads the PARTITION definitions, which only declare() creates - $base is the body
            $constants = $fileColumns->forFile($source, $schema);

            foreach ($reader->batches($source, $batchSize) as $rawBatch) {
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
            $reader = new JsonFileReader(
                $this->filesystem,
                JsonFormat::Document,
                $this->pointer,
                $this->pointerToEntryName,
                iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false),
            );

            $derived =
                $this->derivedSchema = (new SchemaInferrer($this->inference, new InstanceOfTypeNarrower()))->infer(
                    [],
                    $reader->samples($this->inference->sampleSize),
                );
        }

        return $fileColumns->declare($fileColumns->withoutTail($derived));
    }

    public function source(): Path
    {
        return $this->path;
    }

    /**
     * @param string $pointer
     * @param bool $pointerToEntryName - when true pointer will be used as entry name for extracted data
     */
    public function withPointer(string $pointer, bool $pointerToEntryName = false): self
    {
        $this->derivedSchema = null;
        $this->pointer = $pointer;
        $this->pointerToEntryName = $pointerToEntryName;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
