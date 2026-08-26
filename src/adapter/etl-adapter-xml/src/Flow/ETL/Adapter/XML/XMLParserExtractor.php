<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Extractor\Limitable;
use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Extractor\MetadataColumns;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
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
use XMLParser;
use XMLWriter;

use function count;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function sprintf;

final class XMLParserExtractor implements Extractor, FileExtractor, LimitableExtractor, MetadataColumnsExtractor
{
    use MetadataColumns;

    use Limitable;
    use PathFiltering;

    /**
     * @var int<1, max>
     */
    private int $bufferSize = 8096;

    private bool $capturing = false;

    /**
     * @var array<string>
     */
    private array $currentPath = [];

    /**
     * @var array<string>
     */
    private array $elements = [];

    /**
     * @var list<array<string, string>>
     */
    private array $namespaceStack = [];

    private ?XMLParser $parser = null;

    private ?Schema $schema = null;

    private ?XMLWriter $writer = null;

    private string $xmlNodePath = '';

    private readonly Filesystem $filesystem;

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
        $this->resetLimit();
    }

    public function characterDataHandler(XMLParser $parser, string $data): void
    {
        if ($this->capturing) {
            $this->writer()->text($data);
        }
    }

    public function endElementHandler(XMLParser $parser, string $name): void
    {
        if ($this->capturing) {
            $this->writer()->endElement();

            if (
                implode('/', $this->currentPath) === $this->xmlNodePath
                || $this->xmlNodePath === '' && count($this->currentPath) === 1
            ) {
                $this->capturing = false;
                $this->elements[] = $this->writer()->outputMemory();
            }
        }

        array_pop($this->currentPath);
        array_pop($this->namespaceStack);
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

        foreach ((new FileListing($this->filesystem))->list($this->path, $this->filter()) as $listedFile) {
            $stream = $this->filesystem->readFrom($listedFile->path);

            $streamUri = $this->addMetadataColumns ? $stream->path()->uri() : null;
            $partitions = $stream->path()->partitions();

            $schema = $baseSchema;

            foreach ($partitions as $partition) {
                if ($schema->findDefinition($partition->name) === null) {
                    $schema = $schema->add(str_schema($partition->name));
                }
            }

            $rawNodes = [];

            foreach ($stream->iterate($this->bufferSize) as $chunk) {
                if (!xml_parse($this->parser(), $chunk)) {
                    throw new RuntimeException(sprintf(
                        'XML Error: %s at line %d',
                        (string) xml_error_string(xml_get_error_code($this->parser())),
                        xml_get_current_line_number($this->parser()),
                    ));
                }

                foreach ($this->elements as $element) {
                    $rawNodes[] = $element;

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
                                $this->freeParser();

                                return;
                            }
                        }
                    }
                }

                $this->elements = [];
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
                    $this->freeParser();

                    return;
                }
            }

            xml_parse($this->parser(), '', true);

            $this->freeParser();
        }
    }

    public function schema(): Schema
    {
        $schema = $this->schema ?? schema(xml_schema('node'));

        if ($this->addMetadataColumns) {
            return $schema->add(str_schema('_input_file_uri'));
        }

        return $schema;
    }

    public function source(): Path
    {
        return $this->path;
    }

    /**
     * @param array<string, string> $attrs
     */
    public function startElementHandler(XMLParser $parser, string $name, array $attrs): void
    {
        $this->currentPath[] = $name;

        $namespaceDeclarations = [];
        $otherAttributes = [];

        foreach ($attrs as $key => $value) {
            if ($key === 'xmlns' || str_starts_with($key, 'xmlns:')) {
                $namespaceDeclarations[$key] = $value;
            } else {
                $otherAttributes[$key] = $value;
            }
        }

        $this->namespaceStack[] = $namespaceDeclarations;

        $isCapturedRoot =
            implode('/', $this->currentPath) === $this->xmlNodePath
            || $this->xmlNodePath === '' && count($this->currentPath) === 1;

        if ($isCapturedRoot) {
            $this->capturing = true;
            $namespacesToEmit = array_merge(...$this->namespaceStack);
        } elseif ($this->capturing) {
            $namespacesToEmit = $namespaceDeclarations;
        } else {
            return;
        }

        $this->writer()->startElement($name);

        foreach ($namespacesToEmit as $nsKey => $nsValue) {
            $this->writer()->writeAttribute($nsKey, $nsValue);
        }

        foreach ($otherAttributes as $key => $value) {
            $this->writer()->writeAttribute($key, $value);
        }
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

    private function freeParser(): void
    {
        $this->parser = null;
        $this->namespaceStack = [];
        $this->currentPath = [];
    }

    private function parser(): XMLParser
    {
        if ($this->parser === null) {
            $this->parser = xml_parser_create('UTF-8');
            xml_parser_set_option($this->parser, XML_OPTION_CASE_FOLDING, 0);
            xml_set_element_handler($this->parser, $this->startElementHandler(...), $this->endElementHandler(...));
            xml_set_character_data_handler($this->parser, $this->characterDataHandler(...));
        }

        return $this->parser;
    }

    private function writer(): XMLWriter
    {
        if ($this->writer === null) {
            $this->writer = new XMLWriter();
            $this->writer->openMemory();
            $this->writer->setIndent(true);
        }

        return $this->writer;
    }
}
