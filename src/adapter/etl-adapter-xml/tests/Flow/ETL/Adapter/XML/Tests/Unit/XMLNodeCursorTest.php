<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use DOMDocument;
use Flow\ETL\Adapter\XML\XMLNodeCursor;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringSourceStream;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function libxml_clear_errors;
use function libxml_use_internal_errors;

final class XMLNodeCursorTest extends FlowTestCase
{
    /**
     * @param int<1, max> $bufferSize
     * @param list<string> $stops
     */
    #[TestWith(['root/items/item', 1, ['1', '2']])]
    #[TestWith(['root/items/item', 8192, ['1', '2']])]
    #[TestWith(['', 8192, ['root']])]
    #[TestWith(['feed/entry', 8192, []])]
    public function test_stops_on_every_element_on_the_path(string $path, int $bufferSize, array $stops): void
    {
        $seen = [];

        foreach ((new XMLNodeCursor($path))->of(
            new StringSourceStream(
                path('memory://a.xml'),
                '<root><other><items><item id="skipped"/></items></other><items><item id="1"/><item id="2"><item id="nested"/></item></items></root>',
            ),
            $bufferSize,
        ) as $reader) {
            $seen[] = $reader->getAttribute('id') ?? $reader->name;
        }

        static::assertSame($stops, $seen);
    }

    #[TestWith([''])]
    #[TestWith(["<?xml version='1.0'?>"])]
    #[TestWith(['<!-- c -->'])]
    public function test_a_document_without_a_root_has_no_stops(string $xml): void
    {
        static::assertSame(
            [],
            iterator_to_array((new XMLNodeCursor('root/item'))->of(
                new StringSourceStream(path('memory://a.xml'), $xml),
                8192,
            )),
        );
    }

    public function test_a_malformed_document_is_refused_at_its_end(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('XML Error: Extra content at the end of the document at line 1');

        iterator_to_array((new XMLNodeCursor('root/item'))->of(
            new StringSourceStream(path('memory://a.xml'), '<root><item/></root>junk'),
            8192,
        ));
    }

    public function test_libxml_reports_internally_at_every_stop_and_the_callers_mode_returns_after(): void
    {
        $previous = libxml_use_internal_errors(false);

        try {
            $cursor = (new XMLNodeCursor('root/item'))->of(
                new StringSourceStream(path('memory://a.xml'), '<root><item/><item/></root>'),
                8192,
            );

            foreach ($cursor as $_reader) {
                static::assertTrue(libxml_use_internal_errors());
                libxml_use_internal_errors(false);
            }

            static::assertFalse(libxml_use_internal_errors());
        } finally {
            libxml_use_internal_errors($previous);
        }
    }

    public function test_an_abandoned_walk_restores_the_callers_libxml_error_mode(): void
    {
        $previous = libxml_use_internal_errors(false);

        try {
            $cursor = (new XMLNodeCursor('root/item'))->of(
                new StringSourceStream(path('memory://a.xml'), '<root><item/><item/></root>'),
                8192,
            );
            $cursor->current();
            unset($cursor);

            static::assertFalse(libxml_use_internal_errors());
        } finally {
            libxml_use_internal_errors($previous);
        }
    }

    public function test_failure_names_the_last_libxml_error(): void
    {
        $previous = libxml_use_internal_errors(true);

        try {
            (new DOMDocument())->loadXML('<a>');

            static::assertMatchesRegularExpression(
                '/^XML Error: .+ at line 1$/',
                (new XMLNodeCursor('a'))->failure()->getMessage(),
            );
        } finally {
            libxml_clear_errors();
            libxml_use_internal_errors($previous);
        }
    }
}
