<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Floe;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\DerivedFromFiles;
use Flow\ETL\IO\Extractor\File\DerivedSchema;
use Flow\ETL\IO\Extractor\File\FileColumnsSource;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\File\FileSplitReader;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Storage\Floe\FooterReader;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\Codec;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function sprintf;

final class FloeExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    SplittableExtractor
{
    private BatchSize $batchSize;

    private DerivedFromFiles $derived;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

    private ?Schema $schema = null;

    private ?Statistics $statistics = null;

    private bool $unionByName = false;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        private readonly Codec $codec = new NoopCodec(),
        private readonly int $chunkSize = 65536,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'from_floe');

        $this->filesystem = $filesystem;
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
     * @return Generator<int, CorruptedRecordException|Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        yield from $this->reader()->extract($backend, $criteria);
    }

    public function reader(): FileSplitReader
    {
        return new FileSplitReader(
            new FileRead($this->files, $this->fileColumns->columns(), $this->schema(), $this->batchSize()),
            new FloeFileBatches(
                $this->codec,
                $this->chunkSize,
                $this->schema === null && !$this->unionByName
                    ? new DerivedSchema(
                        $this->derived->fold($this->files(), $this->unionByName),
                        $this->derived->from(),
                    )
                    : null,
            ),
        );
    }

    public function schema(): Schema
    {
        $fileColumns = $this->fileColumns->columns();

        if ($this->schema !== null) {
            return $fileColumns->declare($this->schema);
        }

        $derived = $this->derived->fold($this->files(), $this->unionByName);

        if ($fileColumns->withoutTail($derived)->count() === 0) {
            throw InferredSchemaException::nothingToInferFrom('from_floe()', sprintf(
                'no file matching "%s" carries a column',
                $this->path->uri(),
            ));
        }

        return $fileColumns->declare($derived);
    }

    public function statistics(): Statistics
    {
        return $this->statistics ??= $this->declare();
    }

    /**
     * Reconcile every listed file's footer instead of trusting the first one.
     */
    public function unionByName(bool $union = true): self
    {
        $this->unionByName = $union;
        $this->derived = new DerivedFromFiles();
        $this->statistics = null;

        return $this;
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns->columns()->partitions($this->schema());
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
     * Byte windows over every admitted file; a section belongs to the window holding its midpoint.
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
        $this->statistics = null;
    }

    public function source(): Path
    {
        return $this->path;
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
     * The footers schema() reads - the first file's, or every file's under unionByName - read once whichever of the
     * two asks first, and scaled to the listing when only the first was read.
     */
    private function declare(): Statistics
    {
        return $this->derived->footers($this->files(), $this->unionByName)->of($this->files->count());
    }

    /**
     * @return Generator<int, FloeSourceFile>
     */
    private function files(): Generator
    {
        foreach ($this->files->of(new Criteria()) as $source) {
            $stream = $this->filesystem->readFrom($source->path);

            try {
                yield new FloeSourceFile((new FooterReader())->read($stream, $this->codec)->footer, $source);
            } finally {
                $stream->close();
            }
        }
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
