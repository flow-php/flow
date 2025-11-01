<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Value;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Value\HTMLDocument;
use PHPUnit\Framework\Attributes\{DataProvider, RequiresPhp};
use PHPUnit\Framework\TestCase;

final class HTMLDocumentTest extends TestCase
{
    public static function provide_invalid() : \Generator
    {
        yield 'empty' => [
            'value' => '',
        ];

        yield 'invalid' => [
            'value' => 'invalid',
        ];

        yield 'missing doctype' => [
            'value' => '<html><body><div><span>bar</span></div></body></html>',
        ];

        yield 'missing head' => [
            'value' => '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" "http://www.w3.org/TR/REC-html40/loose.dtd">
<html><body><p>invalid</p></body></html>',
        ];

        yield 'missing body' => [
            'value' => '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" "http://www.w3.org/TR/REC-html40/loose.dtd">
<html><head></head></html>',
        ];

        yield 'not closed html' => [
            'value' => '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN" "http://www.w3.org/TR/REC-html40/loose.dtd">
<html><head></head><body><p>invalid</p></body>',
        ];

        yield 'xml' => [
            'value' => '<?xml version="1.0"?><items><item>1</item></items>',
        ];

        yield 'xml element' => [
            'value' => '<items><item>1</item></items>',
        ];
    }

    public static function provide_valid() : \Generator
    {
        yield 'empty head and body' => [
            'value' => '<!DOCTYPE html><html><head></head><body></body></html>',
        ];

        yield 'empty head' => [
            'value' => '<!DOCTYPE html><html><head></head><body><p>invalid</p></body></html>',
        ];

        yield 'empty body' => [
            'value' => '<!DOCTYPE html><html><head><title></title></head><body></body></html>',
        ];

        yield 'full' => [
            'value' => '<!DOCTYPE html><html><head><title></title></head><body><p>invalid</p></body></html>',
        ];
    }

    #[RequiresPhp('>= 8.4')]
    #[DataProvider('provide_valid')]
    public function test_create_with_dom_document_html_on_newer(string $value) : void
    {
        $doc = \Dom\HTMLDocument::createFromString($value);

        $document = new HTMLDocument($doc);

        self::assertSame($value, $document->toString());
    }

    #[RequiresPhp('< 8.4')]
    #[DataProvider('provide_valid')]
    public function test_create_with_dom_document_html_on_old(string $value) : void
    {
        $doc = new \DOMDocument();
        $doc->loadHTML($value);

        $document = new HTMLDocument($doc);

        self::assertSame(
            $value,
            $document->toString(),
        );
    }

    #[RequiresPhp('>= 8.4')]
    #[DataProvider('provide_invalid')]
    public function test_create_with_invalid_html_on_newer(string $value) : void
    {
        $this->expectException(InvalidArgumentException::class);

        new HTMLDocument($value);
    }

    #[RequiresPhp('< 8.4')]
    #[DataProvider('provide_invalid')]
    public function test_create_with_invalid_html_on_old(string $value) : void
    {
        $this->expectException(InvalidArgumentException::class);

        new HTMLDocument($value);
    }

    public function test_with_random_object() : void
    {
        $this->expectException(InvalidArgumentException::class);

        new HTMLDocument(new \stdClass());
    }
}
