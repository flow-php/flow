<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Serializer;

use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Serializer\DomValueCodec;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\ValueDecoder;
use Flow\Types\Type;
use PHPUnit\Framework\Attributes\DataProvider;

use function base64_encode;
use function class_exists;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function gzcompress;

final class DomValueCodecTest extends FlowTestCase
{
    public static function domTypes(): iterable
    {
        yield 'xml' => [type_xml()];
        yield 'xml_element' => [type_xml_element()];
        yield 'html' => [type_html()];
        yield 'html_element' => [type_html_element()];
    }

    public static function nonDomTypes(): iterable
    {
        yield 'string' => [type_string()];
        yield 'integer' => [type_integer()];
    }

    #[DataProvider('domTypes')]
    public function test_handles_dom_types(Type $type): void
    {
        static::assertTrue((new DomValueCodec())->handles($type));
    }

    #[DataProvider('nonDomTypes')]
    public function test_handles_rejects_non_dom_types(Type $type): void
    {
        static::assertFalse((new DomValueCodec())->handles($type));
    }

    #[DataProvider('domTypes')]
    public function test_null_passes_through_untouched(Type $type): void
    {
        static::assertNull((new DomValueCodec())->encode($type, null));
        static::assertNull((new DomValueCodec())->decode($type, null));
    }

    #[DataProvider('nonDomTypes')]
    public function test_non_dom_type_passes_through_untouched(Type $type): void
    {
        static::assertSame('<a>1</a>', (new DomValueCodec())->encode($type, '<a>1</a>'));
        static::assertSame('<a>1</a>', (new DomValueCodec())->decode($type, '<a>1</a>'));
    }

    public function test_xml_document_survives_a_round_trip(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a><b>1</b></a>');

        $codec = new DomValueCodec();

        /** @var DOMDocument $restored */
        $restored = $codec->decode(type_xml(), $codec->encode(type_xml(), $document));

        static::assertInstanceOf(DOMDocument::class, $restored);
        static::assertSame($document->C14N(), $restored->C14N());
    }

    public function test_xml_element_survives_a_round_trip(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a><b>1</b></a>');
        $element = $document->documentElement;
        static::assertInstanceOf(DOMElement::class, $element);

        $codec = new DomValueCodec();

        /** @var DOMElement $restored */
        $restored = $codec->decode(type_xml_element(), $codec->encode(type_xml_element(), $element));

        static::assertInstanceOf(DOMElement::class, $restored);
        static::assertSame('<a><b>1</b></a>', $restored->C14N());
    }

    public function test_a_restored_xml_element_does_not_canonicalize_to_an_empty_string(): void
    {
        $codec = new DomValueCodec();

        $left = new DOMDocument();
        $left->loadXML('<a><b>1</b></a>');
        $right = new DOMDocument();
        $right->loadXML('<z><y>999</y></z>');

        /** @var DOMElement $restoredLeft */
        $restoredLeft = $codec->decode(type_xml_element(), $codec->encode(type_xml_element(), $left->documentElement));
        /** @var DOMElement $restoredRight */
        $restoredRight = $codec->decode(type_xml_element(), $codec->encode(
            type_xml_element(),
            $right->documentElement,
        ));

        static::assertNotSame('', $restoredLeft->C14N());
        static::assertNotSame($restoredLeft->C14N(), $restoredRight->C14N());
    }

    public function test_html_document_survives_a_round_trip(): void
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('\Dom\HTMLDocument requires PHP 8.4+');
        }

        $document = ValueDecoder::htmlDocumentFromString('<p>x</p>');

        $codec = new DomValueCodec();

        static::assertSame(
            type_string()->cast($document),
            type_string()->cast($codec->decode(type_html(), $codec->encode(type_html(), $document))),
        );
    }

    public function test_html_element_survives_a_round_trip(): void
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            static::markTestSkipped('\Dom\HTMLDocument requires PHP 8.4+');
        }

        $element = ValueDecoder::htmlElementFromString('<p>x</p>');

        $codec = new DomValueCodec();

        static::assertSame(
            type_string()->cast($element),
            type_string()->cast($codec->decode(type_html_element(), $codec->encode(type_html_element(), $element))),
        );
    }

    public function test_decode_rejects_a_value_that_is_not_base64(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given value is not valid base64');

        (new DomValueCodec())->decode(type_xml(), '!!! not base64 !!!');
    }

    public function test_decode_rejects_a_value_that_is_not_gzcompressed(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given value is not valid gzcompressed XML');

        (new DomValueCodec())->decode(type_xml(), base64_encode('plain, uncompressed'));
    }

    public function test_decode_rejects_a_payload_that_is_not_xml(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('is not valid XML');

        (new DomValueCodec())->decode(type_xml(), base64_encode((string) gzcompress('not xml at all')));
    }
}
