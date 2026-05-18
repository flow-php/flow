<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\ValueConverter;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\ETL\Adapter\PostgreSql\ValueConverter\HTMLConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use PHPUnit\Framework\TestCase;

use function class_exists;

final class HTMLConverterTest extends TestCase
{
    public function test_delegates_string_to_next_converter(): void
    {
        $converter = new HTMLConverter();

        static::assertSame('plain string', $converter->toDatabase('plain string'));
    }

    public function test_html_document_returns_html_string(): void
    {
        if (!class_exists(HTMLDocument::class)) {
            static::markTestSkipped('Dom\HTMLDocument requires PHP 8.4+');
        }

        $converter = new HTMLConverter();

        $doc = HTMLDocument::createFromString('<!DOCTYPE html><html><body><p>Hello</p></body></html>');
        $result = $converter->toDatabase($doc);

        static::assertIsString($result);
        static::assertStringContainsString('<p>Hello</p>', $result);
    }

    public function test_html_element_returns_html_string(): void
    {
        if (!class_exists(HTMLDocument::class)) {
            static::markTestSkipped('Dom\HTMLDocument requires PHP 8.4+');
        }

        $converter = new HTMLConverter();

        $doc = HTMLDocument::createFromString('<!DOCTYPE html><html><body><p id="test">Hello</p></body></html>');
        $element = $doc->getElementById('test');

        static::assertInstanceOf(HTMLElement::class, $element);

        $result = $converter->toDatabase($element);

        static::assertIsString($result);
        static::assertStringContainsString('<p id="test">Hello</p>', $result);
    }

    public function test_null_returns_null(): void
    {
        $converter = new HTMLConverter();

        static::assertNull($converter->toDatabase(null));
    }

    public function test_supported_types_returns_text(): void
    {
        $converter = new HTMLConverter();

        static::assertSame([ValueType::TEXT], $converter->supportedTypes());
    }
}
