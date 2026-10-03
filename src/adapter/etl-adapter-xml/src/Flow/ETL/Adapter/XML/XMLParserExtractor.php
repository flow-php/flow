<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

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
use Generator;

use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_schema;
use function implode;
use function iterator_to_array;
use function sprintf;

final class XMLParserExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    use Batches;
    use FileReading;

    /**
     * @var int<1, max>
     */
    private int $bufferSize = 8096;

    private ?Schema $schema = null;

    private string $xmlNodePath = '';

    private readonly Filesystem $filesystem;

    private ?ListedFiles $listed = null;

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
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. from_xml($path, filesystem: aws_s3_filesystem(...)).',
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

        yield from (new FileReadLoop(
            $fileColumns,
            $fileColumns->declare($this->schema ?? schema(xml_schema('node'))),
        ))->read(
            iterator_to_array($this->sourceFiles($this->filesystem, $this->path, $pathFilter), false),
            new XMLFileBatches($this->filesystem, $this->xmlNodePath, $this->bufferSize),
            $this->batchSize(),
            $context->backend(),
            new ReadWindow(limit: $limit),
        );
    }

    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->schema ?? schema(xml_schema('node')));
    }

    public function partitionSchema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->partitions($this->schema ?? new Schema());
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function statistics(): Statistics
    {
        $this->listed ??= ListedFiles::of($this->sourceFiles($this->filesystem, $this->path));

        return new Statistics(size: $this->listed->bytes);
    }

    /**
     * @param int<1, max> $bufferSize $bufferSize - size of the chunks to read from the xml file. Bigger chunks means faster reading but more memory usage.
     */
    public function withBufferSize(int $bufferSize): self
    {
        $this->bufferSize = $bufferSize;

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
                $schema->references()->names(),
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
