<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Column\Php\XmlElementPhysical;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

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
        static::assertSame('<root><a>1</a></root>', $physical->toPhysical($element));
        static::assertEquals($element, $physical->fromPhysical('<root><a>1</a></root>'));
        static::assertEquals([$element, null], $physical->fromPhysicalAll(['<root><a>1</a></root>', null]));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_a_dom_element_of_the_new_api_reads_back_as_a_dom_element(): void
    {
        // @mago-ignore analysis:unavailable-method
        $element = XMLDocument::createFromString('<root><a>1</a></root>')->documentElement;
        $physical = new XmlElementPhysical();
        $markup = type_string()->assert($physical->toPhysical($element));

        static::assertSame('<root><a>1</a></root>', $markup);
        static::assertInstanceOf(DOMElement::class, $physical->fromPhysical($markup));
    }
}
