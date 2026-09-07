<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\FileReading;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\RewindableExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
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
    Extractor,
    FileExtractor,
    LimitableExtractor,
    MetadataColumnsExtractor,
    RewindableExtractor
{
    private ?Schema $schema = null;

    use Limitable;
    use FileReading;

    private readonly Filesystem $filesystem;

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
        $this->resetLimit();
    }

    public function isRepeatable(): bool
    {
        return true;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $encoder = new XMLEncoder();

        $baseSchema = $this->schema ?? schema(xml_schema('node'));

        $fileColumns = $this->fileColumns($this->filesystem, $this->path);
        $schema = $fileColumns->declare($baseSchema);

        foreach ($this->sourceFiles($this->filesystem, $this->path) as $source) {
            $constants = $fileColumns->forFile($source, $schema);

            $xmlReader = new XMLReader();
            $xmlReader->open($source->path->path());

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

                    if ($currentPath === $this->xmlNodePath || $this->xmlNodePath === '' && $xmlReader->depth === 0) {
                        $dom = new DOMDocument('1.0', '');
                        $node = $xmlReader->expand($dom);
                        $rawNodes[] = $node === false ? '' : (string) $dom->saveXML($node);

                        if (count($rawNodes) >= $batchSize) {
                            $batch = [];

                            foreach ($encoder->decode($rawNodes) as $rowValues) {
                                $batch[] = new RawRowValues($constants->fill($rowValues->values));
                            }

                            $rawNodes = [];

                            $hydrated = $hydrator->hydrate($batch, $schema);

                            foreach ($hydrated as $hydratedRow) {
                                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                                $this->incrementReturnedRows();

                                if ($signal === Signal::STOP || $this->reachedLimit()) {
                                    $xmlReader->close();

                                    return;
                                }
                            }
                        }
                    }

                    $previousDepth = $xmlReader->depth;
                }
            }

            $batch = [];

            foreach ($encoder->decode($rawNodes) as $rowValues) {
                $batch[] = new RawRowValues($constants->fill($rowValues->values));
            }

            $hydrated = $hydrator->hydrate($batch, $schema);

            foreach ($hydrated as $hydratedRow) {
                $signal = yield Rows::trusted($hydrated->schema(), [$hydratedRow]);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $xmlReader->close();

                    return;
                }
            }

            $xmlReader->close();
        }
    }

    public function schema(): Schema
    {
        return $this->fileColumns($this->filesystem, $this->path)->declare($this->schema ?? schema(xml_schema('node')));
    }

    public function source(): Path
    {
        return $this->path;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
