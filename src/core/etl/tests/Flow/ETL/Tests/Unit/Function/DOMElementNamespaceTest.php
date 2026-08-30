<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Dom\Element;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

final class DOMElementNamespaceTest extends TestCase
{
    private const string XML = <<<'XML'
        <?xml version='1.0' encoding='UTF-8'?>
        <Invoice xmlns:cec="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"
                 xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
                 xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
                 xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2"
                 xmlns:sh="http://www.unece.org/cefact/nodes/StandardBusinessDocumentHeader">
        </Invoice>
        XML;

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_getting_element_namespace(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = XMLDocument::createFromString(self::XML, LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertInstanceOf(Element::class, $element->documentElement);
        static::assertSame('urn:oasis:names:specification:ubl:schema:xsd:Invoice-2', ref('node')
            ->domElementNamespace()
            ->eval(row(['node' => $element->documentElement]), flow_context()));
    }

    public function test_xml_getting_element_namespace(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML(self::XML);

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertSame('urn:oasis:names:specification:ubl:schema:xsd:Invoice-2', ref('node')
            ->domElementNamespace()
            ->eval(row(['node' => $xml->documentElement]), flow_context()));
    }

    public function test_xml_getting_element_non_default_namespace(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML(self::XML);

        static::assertInstanceOf(DOMElement::class, $xml->documentElement);
        static::assertSame('http://www.unece.org/cefact/nodes/StandardBusinessDocumentHeader', ref('node')
            ->domElementNamespace('xmlns:sh')
            ->eval(row(['node' => $xml->documentElement]), flow_context()));
    }
}
