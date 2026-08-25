<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\PathFiltering;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Rows;
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
final class XMLReaderExtractor implements Extractor, FileExtractor, LimitableExtractor
{
    use Limitable;
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
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();
        $hydrator = $context->hydrator();
        $batchSize = $context->config->extractorBatchSize();
        $encoder = new XMLEncoder();

        $baseSchema = schema(xml_schema('node'));

        if ($shouldPutInputIntoRows && $baseSchema->findDefinition('_input_file_uri') === null) {
            $baseSchema = $baseSchema->add(str_schema('_input_file_uri'));
        }

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $streamUri = $shouldPutInputIntoRows ? $listedFile->path->uri() : null;
            $partitions = $listedFile->path->partitions();

            $schema = $baseSchema;

            foreach ($partitions as $partition) {
                if ($schema->findDefinition($partition->name) === null) {
                    $schema = $schema->add(str_schema($partition->name));
                }
            }

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

                                foreach ($partitions as $partition) {
                                    $rowData[$partition->name] = $partition->value;
                                }

                                $batch[] = new RawRowValues($rowData);
                            }

                            $rawNodes = [];

                            foreach ($hydrator->cast($batch, $schema) as $hydratedRow) {
                                $signal = yield Rows::partitioned([$hydratedRow], $partitions);

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

                foreach ($partitions as $partition) {
                    $rowData[$partition->name] = $partition->value;
                }

                $batch[] = new RawRowValues($rowData);
            }

            foreach ($hydrator->cast($batch, $schema) as $hydratedRow) {
                $signal = yield Rows::partitioned([$hydratedRow], $partitions);

                $this->incrementReturnedRows();

                if ($signal === Signal::STOP || $this->reachedLimit()) {
                    $xmlReader->close();

                    return;
                }
            }

            $xmlReader->close();
        }
    }

    public function source(): Path
    {
        return $this->path;
    }
}
