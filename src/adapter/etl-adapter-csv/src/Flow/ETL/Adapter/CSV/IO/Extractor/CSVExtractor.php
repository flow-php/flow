<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\Inference\CSVSniffer;
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
use Flow\ETL\IO\Extractor\File\InferredColumns;
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

final class CSVExtractor implements
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

    private CSVReadOptions $readOptions;

    /**
     * What the last schema inference sampled; null until one ran.
     */
    private ?SampledUnits $sampled = null;

    /**
     * The source the inferred header was read from; '' until an inference ran.
     */

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    private readonly Filesystem $filesystem;

    private CSVDialects $dialects;

    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path) || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'from_csv');

        $this->filesystem = $filesystem;
        $this->inference = new SchemaInference();
        $this->readOptions = new CSVReadOptions();
        $this->dialects = new CSVDialects();
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
        $fileColumns = $this->fileColumns->columns();
        $schema = $this->describe();

        return new FileSplitReader(
            new FileRead($this->files, $fileColumns, $schema, $this->batchSize()),
            new CSVFileBatches(
                new CSVSourceOpener($this->files->filesystem, $this->readOptions, $this->dialects),
                $this->schema === null && !$this->inference->unionByName
                    ? new InferredColumns(
                        $fileColumns->withoutTail($schema),
                        $fileColumns->tail(),
                        $this->inference,
                        $this->derived->from(),
                    )
                    : null,
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
            throw InferredSchemaException::nothingToInferFrom('from_csv()', sprintf(
                'no file matching "%s" carries a column',
                $this->path->uri(),
            ));
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
            $sniffer = new CSVSniffer(
                new CSVSourceOpener($this->filesystem, $this->readOptions, $this->dialects),
                $this->files->of(new Criteria()),
                $this->inference->candidates(),
            );

            $inferred = (new SchemaInferrer($this->inference))->infer($sniffer);
            $derived = $this->derived->learn($inferred->schema, $sniffer->header()->source ?? '');
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
     * Byte windows over every admitted file, cut by arithmetic alone; the read resyncs both ends of a window to a
     * record start (CSVRecordResync), so a quoted newline never splits a record and no record is read twice.
     *
     * @return non-empty-list<FileSplit>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return $this->files->byteWindows($targetSplitBytes, $criteria->pathFilter);
    }

    public function refresh(): void
    {
        $this->files->refresh();
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
    }

    public function source(): Path
    {
        return $this->path;
    }

    /**
     * Rows come from the sample schema inference already read; without one they are unknown, a sniff is never run
     * just for them.
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
            rows: $this->sampled->estimatedRows($this->files->count(), $size),
            size: $size,
        );
    }

    public function withBOMRemoval(bool $removeBOM): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withRemoveBOM($removeBOM);

        return $this;
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    /**
     * @param int $charactersReadInLine - bytes read per step from a remote stream (S3, Azure); never splits a line
     */
    public function withCharactersReadInLine(int $charactersReadInLine): self
    {
        if ($charactersReadInLine < 1) {
            throw new InvalidArgumentException('Characters read in line must be greater than 0');
        }

        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withCharactersReadInLine($charactersReadInLine);

        return $this;
    }

    public function withEmptyToNull(bool $emptyToNull): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withEmptyToNull($emptyToNull);

        return $this;
    }

    public function withEnclosure(string $enclosure): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withEnclosure($enclosure);

        return $this;
    }

    public function withEscape(string $escape): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withEscape($escape);

        return $this;
    }

    public function withHeader(bool $withHeader): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withHeader($withHeader);

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->fileColumns = $this->fileColumns->withMetadataColumns($addMetadataColumns);

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function withSeparator(string $separator): self
    {
        $this->derived = new DerivedFromFiles();
        $this->sampled = null;
        $this->statistics = null;
        $this->dialects = new CSVDialects();
        $this->readOptions = $this->readOptions->withSeparator($separator);

        return $this;
    }
}
