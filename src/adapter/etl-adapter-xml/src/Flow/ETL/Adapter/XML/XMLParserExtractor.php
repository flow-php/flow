<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\ListedFiles;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

use function count;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_schema;
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
        $hydrator = $context->hydrator();
        $batchSize = $this->batchSize();
        $yielded = 0;
        $nodes = new XMLNodes($this->xmlNodePath);

        $baseSchema = $this->schema ?? schema(xml_schema('node'));

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $fileColumns->declare($baseSchema);
        $body = $fileColumns->withoutTail($schema);

        foreach ($this->sourceFiles($this->filesystem, $this->path, $pathFilter) as $source) {
            $stream = $this->filesystem->readFrom($source->path);

            try {
                $constants = $fileColumns->forFile($source, $schema);

                $batch = [];

                foreach ($nodes->of($stream, $this->bufferSize) as $node) {
                    $batch[] = new RawRowValues(['node' => $node]);

                    if (count($batch) >= $batchSize) {
                        $hydrated = $constants->fillRows($hydrator->hydrate($batch, $body), $schema);

                        $batch = [];

                        $yielded += $hydrated->count();

                        $signal = yield $hydrated;

                        if ($signal === Signal::STOP) {
                            return;
                        }

                        if ($limit !== null && $yielded >= $limit) {
                            return;
                        }
                    }
                }

                if ($batch !== []) {
                    $hydrated = $constants->fillRows($hydrator->hydrate($batch, $body), $schema);

                    $yielded += $hydrated->count();

                    $signal = yield $hydrated;

                    if ($signal === Signal::STOP) {
                        return;
                    }

                    if ($limit !== null && $yielded >= $limit) {
                        return;
                    }
                }
            } finally {
                $stream->close();
            }
        }
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

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    public function withXMLNodePath(string $xmlNodePath): self
    {
        $this->xmlNodePath = $xmlNodePath;

        return $this;
    }
}
