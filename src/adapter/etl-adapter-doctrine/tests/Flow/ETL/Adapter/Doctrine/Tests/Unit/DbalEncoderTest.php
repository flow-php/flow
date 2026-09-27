<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use DOMDocument;
use Flow\ETL\Adapter\Doctrine\DbalEncoder;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;

final class DbalEncoderTest extends FlowTestCase
{
    public function test_passes_scalar_values_through_unchanged_preserving_column_order(): void
    {
        $encoded = (new DbalEncoder())->encode(array_to_rows(
            [['id' => 1, 'name' => 'Norbert', 'active' => true]],
            schema(int_schema('id'), str_schema('name'), bool_schema('active')),
        ));

        static::assertSame([['id' => 1, 'name' => 'Norbert', 'active' => true]], $encoded);
        static::assertSame(['id', 'name', 'active'], array_keys($encoded[0]));
    }

    public function test_renders_a_dom_document_value_to_an_xml_string(): void
    {
        $doc = new DOMDocument();
        $doc->loadXML('<profile><bio>Software Engineer</bio></profile>');

        $encoded = (new DbalEncoder())->encode(array_to_rows(
            [['id' => 1, 'profile' => $doc]],
            schema(int_schema('id'), xml_schema('profile')),
        ));

        static::assertSame(1, $encoded[0]['id']);
        static::assertIsString($encoded[0]['profile']);
        static::assertStringContainsString('<bio>Software Engineer</bio>', $encoded[0]['profile']);
    }

    public function test_renders_a_dom_element_value_to_an_xml_string(): void
    {
        $doc = new DOMDocument();
        $doc->loadXML('<root><user><name>John Doe</name></user></root>');
        $element = $doc->getElementsByTagName('user')->item(0);

        $encoded = (new DbalEncoder())->encode(array_to_rows([[
            'user_element' => $element,
        ]], schema(xml_element_schema('user_element'))));

        static::assertIsString($encoded[0]['user_element']);
        static::assertStringContainsString('<name>John Doe</name>', $encoded[0]['user_element']);
    }

    public function test_passes_null_through_unchanged(): void
    {
        static::assertSame(
            [['id' => 1, 'body' => null]],
            (new DbalEncoder())->encode(array_to_rows(
                [['id' => 1, 'body' => null]],
                schema(int_schema('id'), str_schema('body', nullable: true)),
            )),
        );
    }
}
