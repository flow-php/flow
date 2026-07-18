<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use DOMDocument;
use Flow\ETL\Adapter\Doctrine\DbalEncoder;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class DbalEncoderTest extends FlowTestCase
{
    public function test_passes_scalar_values_through_unchanged_preserving_column_order(): void
    {
        $encoded = (new DbalEncoder())->encode([new TypedRowValues(['id' => 1, 'name' => 'Norbert', 'active' => true], [
            'id' => type_integer(),
            'name' => type_string(),
            'active' => type_boolean(),
        ])]);

        static::assertSame([['id' => 1, 'name' => 'Norbert', 'active' => true]], $encoded);
        static::assertSame(['id', 'name', 'active'], array_keys($encoded[0]));
    }

    public function test_renders_a_dom_document_value_to_an_xml_string(): void
    {
        $doc = new DOMDocument();
        $doc->loadXML('<profile><bio>Software Engineer</bio></profile>');

        $encoded = (new DbalEncoder())->encode([new TypedRowValues(['id' => 1, 'profile' => $doc], [
            'id' => type_integer(),
            'profile' => type_xml(),
        ])]);

        static::assertSame(1, $encoded[0]['id']);
        static::assertIsString($encoded[0]['profile']);
        static::assertStringContainsString('<bio>Software Engineer</bio>', $encoded[0]['profile']);
    }

    public function test_renders_a_dom_element_value_to_an_xml_string(): void
    {
        $doc = new DOMDocument();
        $doc->loadXML('<root><user><name>John Doe</name></user></root>');
        $element = $doc->getElementsByTagName('user')->item(0);

        $encoded = (new DbalEncoder())->encode([new TypedRowValues(['user_element' => $element], [
            'user_element' => type_xml_element(),
        ])]);

        static::assertIsString($encoded[0]['user_element']);
        static::assertStringContainsString('<name>John Doe</name>', $encoded[0]['user_element']);
    }

    public function test_passes_null_through_unchanged(): void
    {
        static::assertSame(
            [['id' => 1, 'body' => null]],
            (new DbalEncoder())->encode([new TypedRowValues(['id' => 1, 'body' => null], [
                'id' => type_integer(),
                'body' => type_string(),
            ])]),
        );
    }
}
