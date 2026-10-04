<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Exception\RuntimeException;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\SourceStreamWrapper;
use Generator;
use XMLReader;

use function count;
use function explode;
use function libxml_clear_errors;
use function libxml_get_last_error;
use function libxml_use_internal_errors;
use function sprintf;
use function trim;

use const LIBXML_ERR_FATAL;

/**
 * The walk to every element on a path: the reader stops on each with libxml's internal errors on, so whatever reads the
 * node there reports into failure(), and moves past the node's subtree when resumed.
 */
final readonly class XMLNodeCursor
{
    /**
     * libxml's XML_ERR_DOCUMENT_EMPTY: no root element - an empty, blank or declaration-only file, which yields nothing
     */
    private const int DOCUMENT_EMPTY = 4;

    /**
     * libxml's XML_ERR_DOCUMENT_END, which libxml before 2.12 also reports for a document without a root
     */
    private const int EXTRA_CONTENT = 5;

    /**
     * @var null|non-empty-list<string>
     */
    private ?array $target;

    /**
     * @param string $xmlNodePath slash-separated element names from the root, '' for the root itself
     */
    public function __construct(string $xmlNodePath)
    {
        $this->target = $xmlNodePath === '' ? null : explode('/', $xmlNodePath);
    }

    /**
     * @param int<1, max> $bufferSize bytes read from the stream at a time
     *
     * @return Generator<int, XMLReader>
     */
    public function of(SourceStream $stream, int $bufferSize): Generator
    {
        $depth = $this->target === null ? 0 : count($this->target) - 1;
        $reader = new XMLReader();
        // the buffer, and its last error, are cleared at every node so the end-of-document check sees this read's
        // errors alone
        $internalErrors = libxml_use_internal_errors(true);
        libxml_clear_errors();

        try {
            $moved = $reader->open(SourceStreamWrapper::uri($stream, $bufferSize)) && $reader->read();

            while ($moved) {
                if ($reader->nodeType !== XMLReader::ELEMENT) {
                    $moved = $reader->read();

                    continue;
                }

                // an element off the path is skipped whole, so every ancestor of the cursor is on the path
                if ($this->target !== null && $reader->name !== $this->target[$reader->depth]) {
                    $moved = $reader->next();

                    continue;
                }

                if ($reader->depth < $depth) {
                    $moved = $reader->read();

                    continue;
                }

                yield $reader;

                libxml_use_internal_errors(true);
                libxml_clear_errors();
                $moved = $reader->next();
            }

            $error = libxml_get_last_error();

            if (
                $error !== false
                && $error->level === LIBXML_ERR_FATAL
                && $error->code !== self::DOCUMENT_EMPTY
                && !($error->code === self::EXTRA_CONTENT && (new RootlessDocument())->matches($stream, $bufferSize))
            ) {
                throw $this->failure();
            }
        } finally {
            $reader->close();
            libxml_use_internal_errors($internalErrors);
        }
    }

    public function failure(): RuntimeException
    {
        $error = libxml_get_last_error();

        return new RuntimeException(
            $error === false
                ? 'XML Error: the document could not be read'
                : sprintf('XML Error: %s at line %d', trim($error->message), $error->line),
        );
    }
}
