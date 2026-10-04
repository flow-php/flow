<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use DOMDocument;
use Flow\ETL\Column\Physical\XmlCharacterReferences;
use Flow\Filesystem\SourceStream;
use Generator;

use function libxml_use_internal_errors;

/**
 * libxml builds each node in C, so no element, attribute or text of the document passes through PHP.
 */
final readonly class XMLNodes
{
    private XMLNodeCursor $cursor;

    /**
     * @param string $xmlNodePath slash-separated element names from the root, '' for the root itself
     */
    public function __construct(string $xmlNodePath)
    {
        $this->cursor = new XMLNodeCursor($xmlNodePath);
    }

    /**
     * Each element on the path as a UTF-8 document of its own, declaring every namespace it uses.
     *
     * @param int<1, max> $bufferSize bytes read from the stream at a time
     *
     * @return Generator<int, DOMDocument>
     */
    public function documents(SourceStream $stream, int $bufferSize): Generator
    {
        $internalErrors = libxml_use_internal_errors();

        foreach ($this->cursor->of($stream, $bufferSize) as $reader) {
            $document = new DOMDocument('1.0', 'UTF-8');
            // its PHP warning repeats the libxml error failure() reports
            $node = @$reader->expand($document);

            if ($node === false) {
                throw $this->cursor->failure();
            }

            $document->appendChild($node);
            libxml_use_internal_errors($internalErrors);

            yield $document;
        }
    }

    /**
     * Each element on the path as its outer XML: the text of the document documents() yields for it.
     *
     * @param int<1, max> $bufferSize bytes read from the stream at a time
     *
     * @return Generator<int, string>
     */
    public function texts(SourceStream $stream, int $bufferSize): Generator
    {
        $internalErrors = libxml_use_internal_errors();
        $references = new XmlCharacterReferences();

        foreach ($this->cursor->of($stream, $bufferSize) as $reader) {
            $text = @$reader->readOuterXml();

            if ($text === '') {
                throw $this->cursor->failure();
            }

            libxml_use_internal_errors($internalErrors);

            yield $references->decoded($text);
        }
    }
}
