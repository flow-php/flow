<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Value\HTMLDocument;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

final class HTMLDocumentTest extends TestCase
{
    #[RequiresPhp('>= 8.4')]
    public function test_create_with_dom_document_html_on_newer() : void
    {
        $doc = \Dom\HTMLDocument::createFromString('<html><body><div><span>bar</span></div></body></html>', \LIBXML_HTML_NOIMPLIED);

        $document = new HTMLDocument($doc);

        self::assertSame('<html><body><div><span>bar</span></div></body></html>', (string) $document);
    }

    #[RequiresPhp('< 8.4')]
    public function test_create_with_dom_document_html_on_old() : void
    {
        $doc = new \DOMDocument();
        $doc->loadHTML($html = '<html><body><div><span>bar</span></div></body></html>');

        $document = new HTMLDocument($doc);

        self::assertSame($html, (string) $document);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_create_with_invalid_html_on_newer() : void
    {
        self::markTestSkipped('Skipping invalid test');
        $document = new HTMLDocument('invalid');

        self::assertSame('invalid', (string) $document);
    }

    #[RequiresPhp('< 8.4')]
    public function test_create_with_invalid_html_on_old() : void
    {
        self::markTestSkipped('Skipping invalid test');
        $document = new HTMLDocument('invalid');

        self::assertSame('<p>invalid</p>', (string) $document);
    }

    #[RequiresPhp('>= 8.4')]
    public function test_create_with_proper_html_on_newer() : void
    {
        $html = '<html><body><div><span>bar</span></div></body></html>';
        $document = new HTMLDocument($html);

        self::assertSame($html, (string) $document);
    }

    #[RequiresPhp('< 8.4')]
    public function test_create_with_proper_html_on_old() : void
    {
        $html = '<html><body><div><span>bar</span></div></body></html>';
        $document = new HTMLDocument($html);

        self::assertSame($html, (string) $document);
    }

    public function test_with_random_object() : void
    {
        $this->expectException(InvalidArgumentException::class);

        new HTMLDocument(new \stdClass());
    }
}
