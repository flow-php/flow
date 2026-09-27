<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Adapter\XML\XMLEncoder;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Uuid;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class XMLEncoderTest extends FlowTestCase
{
    public function test_encodes_scalars_into_flat_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><id>1</id><name>Norbert</name></row>',
            $encoder->encode(array_to_rows(
                [['id' => 1, 'name' => 'Norbert']],
                schema(int_schema('id'), str_schema('name')),
            ))[0],
        );
    }

    public function test_encodes_prefixed_columns_as_attributes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row id="1"><name>Norbert</name></row>',
            $encoder->encode(array_to_rows(
                [['_id' => 1, 'name' => 'Norbert']],
                schema(int_schema('_id'), str_schema('name')),
            ))[0],
        );
    }

    public function test_encodes_date_with_the_date_format(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><at>2024-08-01</at></row>',
            $encoder->encode(array_to_rows([[
                'at' => new DateTimeImmutable('2024-08-01 10:00:00'),
            ]], schema(date_schema('at'))))[0],
        );
    }

    public function test_encodes_datetime_with_the_configured_format(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter(), dateTimeFormat: 'Y-m-d');

        static::assertXmlStringEqualsXmlString(
            '<row><at>2024-08-01</at></row>',
            $encoder->encode(array_to_rows([[
                'at' => new DateTimeImmutable('2024-08-01 10:00:00'),
            ]], schema(datetime_schema('at'))))[0],
        );
    }

    public function test_encodes_time_as_microseconds(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><d>3600000000</d></row>',
            $encoder->encode(array_to_rows([['d' => new DateInterval('PT1H')]], schema(time_schema('d'))))[0],
        );
    }

    public function test_encodes_enum_as_its_name(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><e>one</e></row>',
            $encoder->encode(array_to_rows([[
                'e' => BackedIntEnum::one,
            ]], schema(enum_schema('e', BackedIntEnum::class))))[0],
        );
    }

    public function test_encodes_a_timezone_as_its_iana_name(): void
    {
        static::assertXmlStringEqualsXmlString(
            '<row><tz>Europe/Warsaw</tz></row>',
            (new XMLEncoder(new DOMDocumentWriter()))->encode(array_to_rows([[
                'tz' => new DateTimeZone('Europe/Warsaw'),
            ]], schema(time_zone_schema('tz'))))[0],
        );
    }

    public function test_encodes_uuid_as_string(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><id>f47ac10b-58cc-4372-a567-0e02b2c3d479</id></row>',
            $encoder->encode(array_to_rows([[
                'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
            ]], schema(uuid_schema('id'))))[0],
        );
    }

    public function test_encodes_list_into_element_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><tags><element>a</element><element>b</element></tags></row>',
            $encoder->encode(array_to_rows([['tags' => [
                'a',
                'b',
            ]]], schema(list_schema('tags', type_list(type_string())))))[0],
        );
    }

    public function test_encodes_map_into_key_value_element_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><m><element><key>x</key><value>1</value></element></m></row>',
            $encoder->encode(array_to_rows([['m' => [
                'x' => 1,
            ]]], schema(map_schema('m', type_map(type_string(), type_integer())))))[0],
        );
    }

    public function test_encodes_structure_into_nested_nodes(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><address><city>Krakow</city><zip>31-021</zip></address></row>',
            $encoder->encode(array_to_rows([['address' => [
                'city' => 'Krakow',
                'zip' => '31-021',
            ]]], schema(structure_schema('address', type_structure(['city' => type_string(), 'zip' => type_string()])))))[0],
        );
    }

    public function test_encoding_structure_with_optional_elements_throws(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'XML encoder does not support structure optional elements, given: structure{city: string, zip?: string}',
        );

        $encoder->encode(array_to_rows([['address' => [
            'city' => 'Krakow',
        ]]], schema(structure_schema('address', type_structure([
            'city' => type_string(),
            'zip' => structure_element('zip', type_string(), optional: true),
        ])))));
    }

    public function test_encoding_structure_with_an_interleaved_optional_element_throws(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'XML encoder does not support structure optional elements, given: structure{zip?: string, city: string}',
        );

        $encoder->encode(array_to_rows([['address' => [
            'city' => 'Krakow',
        ]]], schema(structure_schema('address', type_structure([
            'zip' => structure_element('zip', type_string(), optional: true),
            'city' => type_string(),
        ])))));
    }

    public function test_encodes_null_scalar_as_an_empty_node(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter());

        static::assertXmlStringEqualsXmlString(
            '<row><name></name></row>',
            $encoder->encode(array_to_rows([['name' => null]], schema(str_schema('name', nullable: true))))[0],
        );
    }

    public function test_uses_the_configured_row_element_name(): void
    {
        $encoder = new XMLEncoder(new DOMDocumentWriter(), rowElementName: 'record');

        static::assertXmlStringEqualsXmlString(
            '<record><id>1</id></record>',
            $encoder->encode(array_to_rows([['id' => 1]], schema(int_schema('id'))))[0],
        );
    }
}
