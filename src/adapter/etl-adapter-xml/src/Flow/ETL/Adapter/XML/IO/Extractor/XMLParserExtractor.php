<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\IO\Extractor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\BatchableExtractor;
use Flow\ETL\IO\Extractor\Criteria;
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
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\XMLDefinition;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;

use function implode;
use function sprintf;

final class XMLParserExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    SplittableExtractor
{
    private BatchSize $batchSize;

    private FileColumnsSource $fileColumns;

    private readonly SourceFiles $files;

    /**
     * @var int<1, max>
     */
    private int $bufferSize = 8096;

    private ?Schema $schema = null;

    private string $xmlNodePath = '';

    /**
     * To iterate only over <element> nodes, use `$loader->withXMLNodePath('root/elements/element')`.
     *
     * <root>
     *   <elements>
     *     <element></element>
     *     <element></element>
     *   <elements>
     * </root>
     *
     * XML Node Path does not support attributes and it's not xpath, it is just a sequence
     * of node names separated with slash.
     *
     * @param Path $path
     */
    public function __construct(
        private readonly Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path) || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'from_xml');

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
            new FileRead($this->files, $this->fileColumns->columns(), $this->schema(), $this->batchSize()),
            new XMLFileBatches($this->xmlNodePath, $this->bufferSize),
        );
    }

    public function schema(): Schema
    {
        return $this->fileColumns->columns()->declare($this->schema ?? new Schema(new XMLDefinition('node')));
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
     * One split per admitted file: a document is a tree with no offset a second reader could start at.
     *
     * @return non-empty-list<FileSplit>
     */
    public function splits(Criteria $criteria, int $targetSplitBytes): array
    {
        return $this->files->perFile($criteria->pathFilter);
    }

    public function refresh(): void
    {
        $this->files->refresh();
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function statistics(): Statistics
    {
        return new Statistics(size: $this->files->bytes());
    }

    public function withBatchSize(int $batchSize): static
    {
        $this->batchSize = new BatchSize($batchSize);

        return $this;
    }

    /**
     * @param int<1, max> $bufferSize $bufferSize - size of the chunks to read from the xml file. Bigger chunks means faster reading but more memory usage.
     */
    public function withBufferSize(int $bufferSize): self
    {
        $this->bufferSize = $bufferSize;

        return $this;
    }

    public function withMetadataColumns(bool $addMetadataColumns): static
    {
        $this->fileColumns = $this->fileColumns->withMetadataColumns($addMetadataColumns);

        return $this;
    }

    /**
     * @throws InvalidArgumentException when $schema has no "node" column, the one the matched elements are read into
     */
    public function withSchema(Schema $schema): static
    {
        if ($schema->findDefinition('node') === null) {
            throw new InvalidArgumentException(sprintf('from_xml() reads every matched element into the "node" column, the schema declares no such column: %s', implode(
                ', ',
                $schema->names(),
            )));
        }

        $this->schema = $schema;

        return $this;
    }

    public function withXMLNodePath(string $xmlNodePath): self
    {
        $this->xmlNodePath = $xmlNodePath;

        return $this;
    }
}
