<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Column\Physical\XmlElementPhysical;
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

final class XmlElementPhysicalTest extends TestCase
{
    public function test_the_markup(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a></root>');
        $element = $document->documentElement;
        $physical = new XmlElementPhysical();

        static::assertNotNull($element);

        $bytes = type_string()->assert($physical->toPhysical($element));

        static::assertSame(
            "\x01\x0021\x00<root><a>1</a></root><?xml version=\"1.0\"?>\n<root><a>1</a></root>\n",
            $bytes,
        );
        static::assertSame('<root><a>1</a></root>', $physical->markup($bytes));
        static::assertEquals($element, $physical->fromPhysical($bytes));
        static::assertEquals([$element, null], $physical->fromPhysicalAll([$bytes, null]));
    }

    public function test_a_round_trip_keeps_the_parent_and_the_siblings(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<root><user><name>Name</name></user><id>01</id></root>');
        $physical = new XmlElementPhysical();
        $user = $document->documentElement?->firstElementChild;

        static::assertNotNull($user);

        $element = type_instance_of(DOMElement::class)->assert($physical->fromPhysical($physical->toPhysical($user)));

        static::assertSame('user', $element->tagName);
        static::assertSame('Name', $element->textContent);
        static::assertSame('root', $element->parentElement?->tagName);
        static::assertSame('01', $element->nextElementSibling?->textContent);
    }

    public function test_bare_element_markup_of_earlier_builds_still_loads(): void
    {
        static::assertSame(
            'Name',
            type_instance_of(DOMElement::class)->assert((new XmlElementPhysical())->fromPhysical(
                '<user><name>Name</name></user>',
            ))->textContent,
        );
    }

    public function test_the_document_without_the_element_markup_still_loads(): void
    {
        $physical = new XmlElementPhysical();
        $bytes = "\x000\x00<root><user><name>Name</name></user><id>01</id></root>";
        $element = type_instance_of(DOMElement::class)->assert($physical->fromPhysical($bytes));

        static::assertSame('user', $element->tagName);
        static::assertSame('root', $element->parentElement?->tagName);
        static::assertSame('<user><name>Name</name></user>', $physical->markup($bytes));
        static::assertSame('<item id="5">value</item>', $physical->markup('<item id="5">value</item>'));
    }

    public function test_a_path_outside_the_document_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Floe failed to restore DOMElement');

        (new XmlElementPhysical())->fromPhysical("\x015\x000\x00<root/>");
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_a_dom_element_of_the_new_api_reads_back_as_a_dom_element(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = XMLDocument::createFromString('<root><a>1</a></root>')->documentElement?->firstElementChild;
        $physical = new XmlElementPhysical();
        $bytes = type_string()->assert($physical->toPhysical($element));

        static::assertSame(
            "\x010\x008\x00<a>1</a><?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<root><a>1</a></root>",
            $bytes,
        );
        static::assertSame(
            'root',
            type_instance_of(DOMElement::class)->assert($physical->fromPhysical($bytes))->parentElement?->tagName,
        );
    }

    public function test_restoring_xml_element_from_string(): void
    {
        static::assertSame(
            'item',
            type_instance_of(DOMElement::class)->assert((new XmlElementPhysical())->fromPhysical(
                '<item id="5">value</item>',
            ))->tagName,
        );
    }

    public function test_a_detached_dom_element_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('failed to convert DOMElement to XML string');

        (new XmlElementPhysical())->toPhysical(new DOMElement('detached'));
    }
}
