<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\File\DerivedSchema;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileReading;
use Flow\ETL\Extractor\File\FileReadLoop;
use Flow\ETL\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Floe\Codec\NoopCodec;
use Generator;

use function iterator_count;
use function iterator_to_array;
use function sprintf;

final class FloeExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    use Batches;
    use FileReading;

    private ?int $offset = null;

    private ?Statistics $statistics = null;

    private bool $unionByName = false;

    private readonly Filesystem $filesystem;

    public function __construct(
        private readonly Path $path,
        private readonly Codec $codec = new NoopCodec(),
        private readonly int $chunkSize = 65536,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_floe($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
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

        yield from (new FileReadLoop($fileColumns, $this->schema()))->read(
            iterator_to_array($this->sourceFiles($this->filesystem, $this->path, $pathFilter), false),
            new FloeFileBatches(
                $this->filesystem,
                $this->codec,
                $this->chunkSize,
                $fileColumns,
                $this->unionByName
                    ? null
                    : new DerivedSchema($this->derivedSchema($this->files(), $this->unionByName), $this->derivedFrom),
            ),
            $this->batchSize(),
            $context->backend(),
            new ReadWindow($this->offset ?? 0, $limit),
        );
    }

    /**
     * Footer-only source schema (two ranged reads per file, no row scan). One file unless
     * unionByName() asks for the fold, and memoised, so repeated calls cost nothing.
     */
    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->derivedSchema(
            $this->files(),
            $this->unionByName,
        ));
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
        $this->derivedSchema = null;
        $this->statistics = null;

        return $this;
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions($this->schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater or equal to 0');
        }

        $this->offset = $offset;
        $this->statistics = null;

        return $this;
    }

    /**
     * The footers schema() reads - the first file's, or every file's under unionByName - read once whichever of the
     * two asks first, and scaled to the listing when only the first was read.
     */
    private function declare(): Statistics
    {
        return $this->derivedFooters($this->files(), $this->unionByName)->of(
            iterator_count($this->sourceFiles($this->filesystem, $this->path)),
            $this->offset ?? 0,
        );
    }

    /**
     * @return Generator<int, FloeSourceFile>
     */
    private function files(): Generator
    {
        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            yield new FloeSourceFile(
                // only the footers are read here, no column is decoded: any backend would do
                (new FloeReader(
                    $this->filesystem,
                    new PhpBackend(),
                    $this->codec,
                    $this->chunkSize,
                ))->read($source->path),
                $source,
            );
        }
    }

    public function withSchema(Schema $schema): static
    {
        throw new InvalidArgumentException(
            'Floe is a self-describing format and does not accept a schema; declaring one is not supported yet.',
        );
    }
}
