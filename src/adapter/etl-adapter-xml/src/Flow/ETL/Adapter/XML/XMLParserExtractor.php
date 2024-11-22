<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\{FileExtractor, Limitable, LimitableExtractor, PathFiltering, Signal};
use Flow\ETL\{Extractor, FlowContext};
use Flow\Filesystem\Path;

final class XMLParserExtractor implements Extractor, FileExtractor, LimitableExtractor
{
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

    private ?\XMLParser $parser = null;

    private ?\XMLWriter $writer = null;

    private string $xmlNodePath = '';

    /**
     * In order to iterate only over <element> nodes use `$loader->withXMLNodePath('root/elements/element')`.
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
    public function __construct(private readonly Path $path)
    {
        $this->resetLimit();
    }

    public function characterDataHandler(\XMLParser $parser, string $data) : void
    {
        if ($this->capturing) {
            $this->writer()->text($data);
        }
    }

    public function endElementHandler(\XMLParser $parser, string $name) : void
    {
        if ($this->capturing) {
            $this->writer()->endElement();

            if (implode('/', $this->currentPath) === $this->xmlNodePath || ($this->xmlNodePath === '' && \count($this->currentPath) === 1)) {
                $this->capturing = false;
                $this->elements[] = $this->writer()->outputMemory();
            }
        }

        array_pop($this->currentPath);
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->list($this->path, $this->filter()) as $stream) {

            foreach ($stream->iterate($this->bufferSize) as $chunk) {
                if (!xml_parse($this->parser(), $chunk)) {
                    throw new RuntimeException(sprintf(
                        'XML Error: %s at line %d',
                        (string) xml_error_string(xml_get_error_code($this->parser())),
                        xml_get_current_line_number($this->parser())
                    ));
                }

                if (\count($this->elements)) {
                    foreach ($this->elements as $element) {
                        if ($shouldPutInputIntoRows) {
                            $rowData = [
                                'node' => $this->createDOMNode($element),
                                '_input_file_uri' => $stream->path()->uri(),
                            ];
                        } else {
                            $rowData = ['node' => $this->createDOMNode($element)];
                        }

                        $signal = yield array_to_rows($rowData, $context->entryFactory(), $stream->path()->partitions());

                        $this->incrementReturnedRows();

                        if ($signal === Signal::STOP || $this->reachedLimit()) {
                            $context->streams()->closeStreams($this->path);
                            $this->freeParser();

                            return;
                        }
                    }
                    $this->elements = [];
                }
            }

            xml_parse($this->parser(), '', true);

            if (\count($this->elements)) {
                foreach ($this->elements as $element) {
                    if ($shouldPutInputIntoRows) {
                        $rowData = [
                            'node' => $this->createDOMNode($element),
                            '_input_file_uri' => $stream->path()->uri(),
                        ];
                    } else {
                        $rowData = ['node' => $this->createDOMNode($element)];
                    }

                    $signal = yield array_to_rows([$rowData], $context->entryFactory(), $stream->path()->partitions());

                    $this->incrementReturnedRows();

                    if ($signal === Signal::STOP || $this->reachedLimit()) {
                        $context->streams()->closeStreams($this->path);
                        $this->freeParser();

                        return;
                    }
                }
                $this->elements = [];
            }

            $this->freeParser();
        }
    }

    public function source() : Path
    {
        return $this->path;
    }

    public function startElementHandler(\XMLParser $parser, string $name, array $attrs) : void
    {
        $this->currentPath[] = $name;
        $currentPathString = implode('/', $this->currentPath);

        if ($currentPathString === $this->xmlNodePath || ($this->xmlNodePath === '' && \count($this->currentPath) === 1)) {
            $this->capturing = true;
            $this->writer()->startElement($name);

            foreach ($attrs as $key => $value) {
                $this->writer()->writeAttribute($key, $value);
            }
        } elseif ($this->capturing) {
            $this->writer()->startElement($name);

            foreach ($attrs as $key => $value) {
                $this->writer()->writeAttribute($key, $value);
            }
        }
    }

    /**
     * @param int<1, max> $bufferSize $bufferSize - size of the chunks to read from the xml file. Bigger chunks means faster reading but more memory usage.
     */
    public function withBufferSize(int $bufferSize) : self
    {
        $this->bufferSize = $bufferSize;

        return $this;
    }

    public function withXMLNodePath(string $xmlNodePath) : self
    {
        $this->xmlNodePath = $xmlNodePath;

        return $this;
    }

    private function createDOMNode(string $xmlString) : \DOMNode
    {
        $doc = new \DOMDocument();
        $doc->loadXML($xmlString);

        return $doc;
    }

    private function freeParser() : void
    {
        if ($this->parser !== null) {
            xml_parser_free($this->parser);
            $this->parser = null;
        }
    }

    private function parser() : \XMLParser
    {
        if ($this->parser === null) {
            $this->parser = xml_parser_create();
            xml_parser_set_option($this->parser, XML_OPTION_CASE_FOLDING, 0);
            xml_set_element_handler($this->parser, [$this, 'startElementHandler'], [$this, 'endElementHandler']);
            xml_set_character_data_handler($this->parser, [$this, 'characterDataHandler']);
        }

        return $this->parser;
    }

    private function writer() : \XMLWriter
    {
        if ($this->writer === null) {
            $this->writer = new \XMLWriter();
            $this->writer->openMemory();
            $this->writer->setIndent(true);
        }

        return $this->writer;
    }
}
