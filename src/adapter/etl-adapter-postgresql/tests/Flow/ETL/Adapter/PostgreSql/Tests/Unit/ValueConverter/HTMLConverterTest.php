<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\ValueConverter;

use Dom\{HTMLDocument, HTMLElement};
use Flow\ETL\Adapter\PostgreSql\ValueConverter\HTMLConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

final class HTMLConverterTest extends TestCase
{
    public function test_delegates_string_to_next_converter() : void
    {
        $converter = new HTMLConverter();

        self::assertSame('plain string', $converter->toDatabase('plain string'));
    }

    public function test_html_document_returns_html_string() : void
    {
        if (!\class_exists(HTMLDocument::class)) {
            self::markTestSkipped('Dom\HTMLDocument requires PHP 8.4+');
        }

        $converter = new HTMLConverter();

        $doc = HTMLDocument::createFromString('<html><body><p>Hello</p></body></html>');
        $result = $converter->toDatabase($doc);

        self::assertIsString($result);
        self::assertStringContainsString('<p>Hello</p>', $result);
    }

    public function test_html_element_returns_html_string() : void
    {
        if (!\class_exists(HTMLDocument::class)) {
            self::markTestSkipped('Dom\HTMLDocument requires PHP 8.4+');
        }

        $converter = new HTMLConverter();

        $doc = HTMLDocument::createFromString('<html><body><p id="test">Hello</p></body></html>');
        $element = $doc->getElementById('test');

        self::assertInstanceOf(HTMLElement::class, $element);

        $result = $converter->toDatabase($element);

        self::assertIsString($result);
        self::assertStringContainsString('<p id="test">Hello</p>', $result);
    }

    public function test_null_returns_null() : void
    {
        $converter = new HTMLConverter();

        self::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_returns_text() : void
    {
        $converter = new HTMLConverter();

        self::assertSame([ValueType::TEXT], $converter->supportedTypes());
    }
}
