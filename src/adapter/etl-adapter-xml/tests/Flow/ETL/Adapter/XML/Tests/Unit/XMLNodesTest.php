<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use DOMDocument;
use Flow\ETL\Adapter\XML\Tests\Context\SerializedNodes;
use Flow\ETL\Adapter\XML\XMLNodes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Stream\StringSourceStream;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Filesystem\DSL\path;
use function libxml_use_internal_errors;

final class XMLNodesTest extends FlowTestCase
{
    /**
     * @param int<1, max> $bufferSize
     */
    #[TestWith([1])]
    #[TestWith([8192])]
    public function test_yields_each_element_on_the_path_as_its_own_document(int $bufferSize): void
    {
        static::assertSame(
            ['<item id="1">a</item>', '<item id="2"><b>b</b></item>'],
            SerializedNodes::of(
                'root/items/item',
                '<root><items><item id="1">a</item><item id="2"><b>b</b></item></items></root>',
                $bufferSize,
            ),
        );
    }

    public function test_an_element_off_the_path_is_skipped_with_its_subtree(): void
    {
        static::assertSame(
            ['<item>kept</item>'],
            SerializedNodes::of(
                'root/items/item',
                '<root><other><items><item>skipped</item></items></other><items><item>kept</item></items></root>',
            ),
        );
    }

    public function test_an_empty_path_yields_the_root(): void
    {
        static::assertSame(['<root><item>1</item></root>'], SerializedNodes::of('', '<root><item>1</item></root>'));
    }

    public function test_a_path_the_document_lacks_yields_nothing(): void
    {
        static::assertSame([], SerializedNodes::of('feed/entry', '<root><item>1</item></root>'));
    }

    /**
     * @param int<1, max> $bufferSize
     */
    #[TestWith(['', 8192])]
    #[TestWith(["  \n ", 8192])]
    #[TestWith(["<?xml version='1.0'?>", 8192])]
    #[TestWith(["<?xml version='1.0'?>\n \n", 1])]
    #[TestWith(['<!-- c -->', 8192])]
    #[TestWith(["\xEF\xBB\xBF", 8192])]
    public function test_a_document_without_a_root_yields_nothing(string $xml, int $bufferSize): void
    {
        static::assertSame([], SerializedNodes::of('root/item', $xml, $bufferSize));
    }

    public function test_a_node_declares_the_ancestor_namespaces_it_uses(): void
    {
        static::assertSame(
            ['<entry xmlns:g="http://g"><g:id>1</g:id></entry>'],
            SerializedNodes::of(
                'feed/entry',
                '<feed xmlns:g="http://g" xmlns:unused="http://unused"><entry><g:id>1</g:id></entry></feed>',
            ),
        );
    }

    public function test_a_node_keeps_the_source_whitespace_and_comments(): void
    {
        static::assertSame(
            ["<item>\n  <!-- note -->\n  <id>1</id>\n</item>"],
            SerializedNodes::of('root/item', "<root>\n<item>\n  <!-- note -->\n  <id>1</id>\n</item>\n</root>"),
        );
    }

    public function test_an_undeclared_prefix_is_read_as_a_plain_name(): void
    {
        static::assertSame(['<g:a>1</g:a>'], SerializedNodes::of('root/g:a', '<root><g:a>1</g:a></root>'));
    }

    #[TestWith([
        '<root><item>1</item><item>2</wrong></root>',
        '/^XML Error: Opening and ending tag mismatch: item line 1 and wrong at line 1$/',
    ])]
    // older libxml reports an unclosed root as extra content
    #[TestWith([
        '<root><item>1</item>',
        '/^XML Error: (Premature end of data in tag root line 1|Extra content at the end of the document) at line 1$/',
    ])]
    #[TestWith(['<root/>junk', '/^XML Error: Extra content at the end of the document at line 1$/'])]
    #[TestWith([
        "<?xml version='1.0'?><root/>junk",
        '/^XML Error: Extra content at the end of the document at line 1$/',
    ])]
    public function test_a_malformed_document_is_refused(string $xml, string $message): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches($message);

        SerializedNodes::of('root/item', $xml);
    }

    public function test_an_error_libxml_holds_from_before_does_not_fail_the_read(): void
    {
        $previous = libxml_use_internal_errors(true);

        try {
            (new DOMDocument())->loadXML('<broken>');

            static::assertSame(['<item>1</item>'], SerializedNodes::of('root/item', '<root><item>1</item></root>'));
        } finally {
            libxml_use_internal_errors($previous);
        }
    }

    #[TestWith([false])]
    #[TestWith([true])]
    public function test_the_callers_libxml_error_mode_holds_at_every_yield_and_after(bool $internalErrors): void
    {
        $previous = libxml_use_internal_errors($internalErrors);

        try {
            $nodes = (new XMLNodes('root/item'))->of(
                new StringSourceStream(path('memory://a.xml'), '<root><item>1</item><item>2</item></root>'),
                8192,
            );

            foreach ($nodes as $_node) {
                static::assertSame($internalErrors, libxml_use_internal_errors());
            }

            static::assertSame($internalErrors, libxml_use_internal_errors());
        } finally {
            libxml_use_internal_errors($previous);
        }
    }

    public function test_an_abandoned_read_restores_the_callers_libxml_error_mode(): void
    {
        $previous = libxml_use_internal_errors(false);

        try {
            $nodes = (new XMLNodes('root/item'))->of(
                new StringSourceStream(path('memory://a.xml'), '<root><item>1</item><item>2</item></root>'),
                8192,
            );
            $nodes->current();
            unset($nodes);

            static::assertFalse(libxml_use_internal_errors());
        } finally {
            libxml_use_internal_errors($previous);
        }
    }
}
