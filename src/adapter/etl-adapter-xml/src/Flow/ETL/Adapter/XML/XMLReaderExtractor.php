<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\DeclaresPartitionTypes;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumns;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Generator;
use XMLReader;

use function array_pop;
use function count;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function implode;
use function sprintf;

/**
 * @deprecated Use XMLParserExtractor instead, XMLReaderExtractor can't properly handle reading remote files since it requires a local file.
 */
final class XMLReaderExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    private ?Schema $schema = null;

    use MetadataColumns;

    use Limitable;
    use DeclaresPartitionTypes;
    use PathFiltering;

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

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $encoder = new XMLEncoder();

        $baseSchema = $this->schema();

        $partitionColumns = new PartitionColumns($this->filesystem);
        $partitionNames = $this->partitionNames($partitionColumns, $this->path);

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $streamUri = $this->addMetadataColumns ? $listedFile->path->uri() : null;
            $partitions = $listedFile->path->partitions();
            $partitionValues = [];

            foreach ($partitions as $partition) {
                $partitionValues[$partition->name] = $partition->value;
            }

            $schema = $baseSchema;

            $schema = $partitionColumns->declare($schema, $partitionNames, $this->declaredPartitionTypes());

            $xmlReader = new XMLReader();
            $xmlReader->open($listedFile->path->path());

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
                                $rowData = $rowValues->values;

                                if ($streamUri !== null) {
                                    $rowData['_input_file_uri'] = $streamUri;
                                }

                                $rowData = $partitionColumns->fill($rowData, $partitionNames, $partitionValues);

                                $batch[] = new RawRowValues($rowData);
                            }

                            $rawNodes = [];

                            $hydrated = $hydrator->cast($batch, $schema);

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
                $rowData = $rowValues->values;

                if ($streamUri !== null) {
                    $rowData['_input_file_uri'] = $streamUri;
                }

                $rowData = $partitionColumns->fill($rowData, $partitionNames, $partitionValues);

                $batch[] = new RawRowValues($rowData);
            }

            $hydrated = $hydrator->cast($batch, $schema);

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
        $schema = $this->schema ?? schema(xml_schema('node'));

        $partitionColumns = new PartitionColumns($this->filesystem);

        return $partitionColumns->declare(
            $this->addMetadataColumns ? $schema->add(str_schema('_input_file_uri')) : $schema,
            $this->partitionNames($partitionColumns, $this->path),
        );
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
