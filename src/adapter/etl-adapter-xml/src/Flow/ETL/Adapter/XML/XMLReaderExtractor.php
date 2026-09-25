<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use DOMDocument;
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
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;
use XMLReader;

use function array_pop;
use function count;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_schema;
use function implode;
use function sprintf;

/**
 * @deprecated Use XMLParserExtractor instead, XMLReaderExtractor can't properly handle reading remote files since it requires a local file.
 */
final class XMLReaderExtractor implements
    BatchableExtractor,
    Extractor,
    FileExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    private ?Schema $schema = null;

    use Batches;
    use FileReading;

    private readonly Filesystem $filesystem;

    private ?ListedFiles $listed = null;

    /**
     * In order to iterate only over <element> nodes us root/elements/element.
     *
     * <root>
     *   <elements>
     *     <element></element>
     *     <element></element>
     *   <elements>
     * </root>
     *
     * $xmlNodePath does not support attributes and it's not xpath, it is just a sequence
     * of node names separated with slash.
     *
     * @param string $xmlNodePath
     */
    public function __construct(
        private readonly Path $path,
        private readonly string $xmlNodePath = '',
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

        if (!$this->path->isLocal()) {
            throw new InvalidArgumentException(
                'XMLReaderExtractor supports only local files, please use XMLParserExtractor that depends on php-xml extension.',
            );
        }
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
        $encoder = new XMLEncoder();

        $baseSchema = $this->schema ?? schema(xml_schema('node'));

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $fileColumns->declare($baseSchema);
        $body = $fileColumns->withoutTail($schema);

        foreach ($this->sourceFiles($this->filesystem, $this->path, $pathFilter) as $source) {
            $constants = $fileColumns->forFile($source, $schema);

            $xmlReader = new XMLReader();
            $xmlReader->open($source->path->path());

            try {
                $previousDepth = 0;
                $currentPathBreadCrumbs = [];

                $rawNodes = [];

                while ($xmlReader->read()) {
                    if ($xmlReader->nodeType === XMLReader::ELEMENT) {
                        if ($previousDepth === $xmlReader->depth) {
                            array_pop($currentPathBreadCrumbs);
                            $currentPathBreadCrumbs[] = $xmlReader->name;
                        }

                        if ($xmlReader->depth > $previousDepth) {
                            $currentPathBreadCrumbs[] = $xmlReader->name;
                        }

                        while ($xmlReader->depth < $previousDepth) {
                            array_pop($currentPathBreadCrumbs);
                            $previousDepth--;
                        }

                        $currentPath = implode('/', array_map(strval(...), $currentPathBreadCrumbs));

                        if (
                            $currentPath === $this->xmlNodePath
                            || $this->xmlNodePath === '' && $xmlReader->depth === 0
                        ) {
                            $dom = new DOMDocument('1.0', '');
                            $node = $xmlReader->expand($dom);
                            $rawNodes[] = $node === false ? '' : (string) $dom->saveXML($node);

                            if (count($rawNodes) >= $batchSize) {
                                $hydrated = $constants->fillRows(
                                    $hydrator->hydrate($encoder->decode($rawNodes), $body),
                                    $schema,
                                );

                                $rawNodes = [];

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

                        $previousDepth = $xmlReader->depth;
                    }
                }

                if ($rawNodes !== []) {
                    $hydrated = $constants->fillRows($hydrator->hydrate($encoder->decode($rawNodes), $body), $schema);

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
                $xmlReader->close();
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

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
