<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\XML\XMLEncoder;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;

final class XMLEncoderTest extends FlowTestCase
{
    public function test_decode_wraps_raw_xml_into_a_node_column(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertSame(
            ['node' => '<root><foo>1</foo></root>'],
            $encoder->decode(['<root><foo>1</foo></root>'])[0]->values,
        );
    }

    public function test_encodes_scalars_into_flat_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><id>1</id><name>Norbert</name></row>',
            $encoder->encode([new TypedRowValues(['id' => 1, 'name' => 'Norbert'], [
                'id' => type_integer(),
                'name' => type_string(),
            ])])[0],
        );
    }

    public function test_encodes_prefixed_columns_as_attributes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row id="1"><name>Norbert</name></row>',
            $encoder->encode([new TypedRowValues(['_id' => 1, 'name' => 'Norbert'], [
                '_id' => type_integer(),
                'name' => type_string(),
            ])])[0],
        );
    }

    public function test_encodes_date_with_the_date_format(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><at>2024-08-01</at></row>',
            $encoder->encode([new TypedRowValues(['at' => new DateTimeImmutable('2024-08-01 10:00:00')], [
                'at' => type_date(),
            ])])[0],
        );
    }

    public function test_encodes_datetime_with_the_configured_format(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter(), dateTimeFormat: 'Y-m-d');

        static::assertXmlStringEqualsXmlString(
            '<row><at>2024-08-01</at></row>',
            $encoder->encode([new TypedRowValues(['at' => new DateTimeImmutable('2024-08-01 10:00:00')], [
                'at' => type_datetime(),
            ])])[0],
        );
    }

    public function test_encodes_time_as_microseconds(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><d>3600000000</d></row>',
            $encoder->encode([new TypedRowValues(['d' => new DateInterval('PT1H')], ['d' => type_time()])])[0],
        );
    }

    public function test_encodes_enum_as_its_name(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><e>one</e></row>',
            $encoder->encode([new TypedRowValues(['e' => BackedIntEnum::one], [
                'e' => type_enum(BackedIntEnum::class),
            ])])[0],
        );
    }

    public function test_encodes_a_timezone_as_its_iana_name(): void
    {
        static::assertXmlStringEqualsXmlString(
            '<row><tz>Europe/Warsaw</tz></row>',
            (new XMLEncoder(new DOMDocumentWriter()))->encode([new TypedRowValues([
                'tz' => new DateTimeZone('Europe/Warsaw'),
            ], ['tz' => type_time_zone()])])[0],
        );
    }

    public function test_encodes_uuid_as_string(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><id>f47ac10b-58cc-4372-a567-0e02b2c3d479</id></row>',
            $encoder->encode([new TypedRowValues(['id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')], [
                'id' => type_uuid(),
            ])])[0],
        );
    }

    public function test_encodes_list_into_element_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><tags><element>a</element><element>b</element></tags></row>',
            $encoder->encode([new TypedRowValues(['tags' => ['a', 'b']], ['tags' => type_list(type_string())])])[0],
        );
    }

    public function test_encodes_map_into_key_value_element_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><m><element><key>x</key><value>1</value></element></m></row>',
            $encoder->encode([new TypedRowValues(['m' => ['x' => 1]], ['m' => type_map(
                type_string(),
                type_integer(),
            )])])[0],
        );
    }

    public function test_encodes_structure_into_nested_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><address><city>Krakow</city><zip>31-021</zip></address></row>',
            $encoder->encode([new TypedRowValues(['address' => [
                'city' => 'Krakow',
                'zip' => '31-021',
            ]], ['address' => type_structure(['city' => type_string(), 'zip' => type_string()])])])[0],
        );
    }

    public function test_encoding_structure_with_optional_elements_throws(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'XML encoder does not support structure optional elements, given: structure{city: string, zip?: string}',
        );

        $encoder->encode([new TypedRowValues(['address' => ['city' => 'Krakow']], ['address' => type_structure([
            'city' => type_string(),
        ], ['zip' => type_string()])])]);
    }

    public function test_encodes_null_scalar_as_an_empty_node(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><name></name></row>',
            $encoder->encode([new TypedRowValues(['name' => null], ['name' => type_string()])])[0],
        );
    }

    public function test_uses_the_configured_row_element_name(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter(), rowElementName: 'record');

        static::assertXmlStringEqualsXmlString(
            '<record><id>1</id></record>',
            $encoder->encode([new TypedRowValues(['id' => 1], ['id' => type_integer()])])[0],
        );
    }
}
