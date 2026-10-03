<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JSONMachine;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileReading;
use Flow\ETL\Extractor\File\FileReadLoop;
use Flow\ETL\Extractor\File\ListedFiles;
use Flow\ETL\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\InfersSchema;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferenceBuilder;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Types\Type\Logical\InstanceOfTypeNarrower;
use Generator;

use function iterator_to_array;
use function sprintf;

final class JsonExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    InfersSchema,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    use Batches;
    use FileReading;

    private SchemaInference $inference;

    private ?string $pointer = null;

    private bool $pointerToEntryName = false;

    /**
     * What the last schema inference sampled; null until one ran.
     */
    private ?JsonSampledFiles $sampled = null;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    private readonly Filesystem $filesystem;

    private ?ListedFiles $listed = null;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        private readonly JsonFormat $format = JsonFormat::Document,
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. %s($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
                $format === JsonFormat::Lines ? 'from_json_lines' : 'from_json',
            ));
        }

        $this->filesystem = $filesystem;
        $this->inference = new SchemaInference();
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null, Filter $pathFilter = new OnlyFiles()): Generator
    {
        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $this->schema();
        $sources = iterator_to_array($this->sourceFiles($this->filesystem, $this->path, $pathFilter), false);

        yield from (new FileReadLoop($fileColumns, $schema))->read(
            $sources,
            new JsonFileBatches(
                $this->filesystem,
                new JsonFileReader(
                    $this->filesystem,
                    $this->format,
                    $this->pointer,
                    $this->pointerToEntryName,
                    $sources,
                ),
                $this->format,
                $this->pointer,
            ),
            $this->batchSize(),
            $context->backend(),
            new ReadWindow(limit: $limit),
        );
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->derivedSchema = null;
        $this->sampled = null;
        $this->statistics = null;

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
                $this->format,
                $this->pointer,
                $this->pointerToEntryName,
                iterator_to_array($this->sourceFiles($this->filesystem, $this->path), false),
            );

            $samples = new JsonSamples($reader->samples($this->inference->sampleSize));
            $derived =
                $this->derivedSchema = (new SchemaInferrer($this->inference, new InstanceOfTypeNarrower()))->infer(
                    [],
                    $samples,
                );
            $this->sampled = $samples->sampledFiles();
        }

        return $fileColumns->declare($fileColumns->withoutTail($derived));
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions($this->schema ?? new Schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    /**
     * Rows come from what the sample schema inference already read; without one they are unknown, a sample is never
     * read just for them. A document has no bytes-per-row unit, so its rows are known only when that sample read every
     * element of every file; JSON lines estimate them from the sampled bytes per row.
     */
    public function statistics(): Statistics
    {
        if ($this->statistics !== null) {
            return $this->statistics;
        }

        $this->listed ??= ListedFiles::of($this->sourceFiles($this->filesystem, $this->path));
        $size = $this->listed->bytes;

        if ($this->sampled === null) {
            return new Statistics(rows: Cardinality::unknown(), size: $size);
        }

        return $this->statistics = new Statistics(
            rows: $this->format === JsonFormat::Document
                ? $this->sampled->exactRows($this->listed->count)
                : $this->sampled->estimatedRows($this->listed->count, $size),
            size: $size,
        );
    }

    /**
     * @param string $pointer
     * @param bool $pointerToEntryName - when true pointer will be used as entry name for extracted data
     */
    public function withPointer(string $pointer, bool $pointerToEntryName = false): self
    {
        $this->derivedSchema = null;
        $this->sampled = null;
        $this->statistics = null;
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
