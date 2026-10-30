<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Inference\JsonSniffer;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedFromFiles;
use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Extractor\InfersSchema;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\IO\Inference\SampledUnits;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function sprintf;

final class JsonExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    InfersSchema,
    MetadataColumnsExtractor,
    SplittableExtractor
{
    private BatchSize $batchSize;

    private DerivedFromFiles $derived;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

    private SchemaInference $inference;

    private ?string $pointer = null;

    private bool $pointerToColumnName = false;

    /**
     * What the last schema inference sampled; null until one ran.
     */
    private ?SampledUnits $sampled = null;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        private readonly JsonFraming $format = JsonFraming::Array,
    ) {
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath(
                $filesystem,
                $path,
                $format === JsonFraming::Lines ? 'from_json_lines' : 'from_json',
            );

        $this->filesystem = $filesystem;
        $this->inference = new SchemaInference();
        $this->derived = new DerivedFromFiles();
        $this->files = new SourceFiles($filesystem, $this->path);
        $this->fileColumns = new FileColumnsSource($this->files);
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
        yield from $this->reader()->extract($backend, $criteria);
    }

    public function reader(): FileSplitReader
    {
        return new FileSplitReader(
            new FileRead($this->files, $this->fileColumns->columns(), $this->describe(), $this->batchSize()),
            new JsonFileBatches(
                $this->files->filesystem,
                new JsonFileReader($this->files->filesystem, $this->format, $this->pointer, $this->pointerToColumnName),
            ),
        );
    }

    public function inferSchema(SchemaInferenceBuilder $builder): static
    {
        $this->inference = $builder->build();
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;

        return $this;
    }

    public function schema(): Schema
    {
        $schema = $this->describe();
        $fileColumns = $this->fileColumns->columns();

        if ($this->schema === null && $fileColumns->withoutTail($schema)->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom(
                $this->format === JsonFraming::Lines ? 'from_json_lines()' : 'from_json()',
                sprintf('no file matching "%s" carries a column', $this->path->uri()),
            );
        }

        return $schema;
    }

    private function describe(): Schema
    {
        $fileColumns = $this->fileColumns->columns();

        if ($this->schema !== null) {
            return $fileColumns->declare($this->schema);
        }

        $derived = $this->derived->schema();

        if ($derived === null) {
            $inferred = (new SchemaInferrer($this->inference))->infer(
                new JsonSniffer(
                    new JsonFileReader($this->filesystem, $this->format, $this->pointer, $this->pointerToColumnName),
                    $this->files->of(new Criteria()),
                ),
            );
            $derived = $this->derived->learn($inferred->schema);
            $this->sampled = $inferred->units;
        }

        return $fileColumns->declare($fileColumns->withoutTail($derived));
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns->columns()->partitions($this->schema ?? new Schema());
    }

    public function partitionTypes(PartitionTypes $types): static
    {
        $this->fileColumns = $this->fileColumns->withPartitionTypes($types);

        return $this;
    }

    public function prunes(ScalarFunction $conjunct): bool
    {
        return false;
    }

    /**
     * JSON lines are cut into byte windows, both ends resynced to a line start by the reader; a JSON document is a
     * tree with no offset a second reader could start at, so it is one split per admitted file.
     *
     * @return non-empty-list<FileSplit>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return $this->format === JsonFraming::Lines
            ? $this->files->byteWindows($targetSplitBytes, $criteria->pathFilter)
            : $this->files->perFile($criteria->pathFilter);
    }

    public function refresh(): void
    {
        $this->files->refresh();
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
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
        $size = $this->files->bytes();

        if ($this->sampled === null) {
            return new Statistics(rows: Cardinality::unknown(), size: $size);
        }

        return $this->statistics = new Statistics(
            rows: $this->format !== JsonFraming::Lines
                ? $this->sampled->exactRows($this->files->count())
                : $this->sampled->estimatedRows($this->files->count(), $size),
            size: $size,
        );
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->fileColumns = $this->fileColumns->withMetadataColumns($addMetadataColumns);

        return $this;
    }

    /**
     * @param string $pointer
     * @param bool $pointerToColumnName - when true pointer will be used as column name for extracted data
     */
    public function withPointer(string $pointer, bool $pointerToColumnName = false): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->pointer = $pointer;
        $this->pointerToColumnName = $pointerToColumnName;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
