<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Unit;

use DateTimeImmutable;
use DOMException;
use Flow\ETL\Adapter\XML\XMLEncoder;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Adapter\XML\XMLWriter\StringXMLWriter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\CastingException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class XMLEncoderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{list<Definition<mixed>>, list<array<string, mixed>>, string}>
     */
    public static function batches(): Generator
    {
        yield 'scalars' => [
            [int_schema('id'), str_schema('name')],
            [['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => 'a < b & c']],
            "<row><id>1</id><name>Norbert</name></row>\n<row><id>2</id><name>a &lt; b &amp; c</name></row>\n",
        ];
        yield 'a prefixed column is an attribute' => [
            [int_schema('_id'), str_schema('name')],
            [['_id' => 1, 'name' => 'Norbert']],
            "<row id=\"1\"><name>Norbert</name></row>\n",
        ];
        yield 'attributes only' => [
            [int_schema('_id'), str_schema('_name'), bool_schema('_active'), float_schema('_price')],
            [['_id' => 1, '_name' => 'a "b"', '_active' => true, '_price' => 1.0]],
            "<row id=\"1\" name=\"a &quot;b&quot;\" active=\"true\" price=\"1.0\"/>\n",
        ];
        yield 'an attribute of a date, a datetime and a list' => [
            [date_schema('_on'), datetime_schema('_at'), list_schema('_tags', type_list(type_string()))],
            [[
                '_on' => new DateTimeImmutable('2024-08-01'),
                '_at' => new DateTimeImmutable('2024-08-01 10:00:00 UTC'),
                '_tags' => ['a', 'b'],
            ]],
            "<row on=\"2024-08-01\" at=\"2024-08-01T10:00:00.000000+00:00\" tags=\"[&quot;a&quot;,&quot;b&quot;]\"/>\n",
        ];
        yield 'a null scalar is an empty element' => [
            [str_schema('name', nullable: true)],
            [['name' => null]],
            "<row><name></name></row>\n",
        ];
        yield 'list' => [
            [list_schema('tags', type_list(type_string()), nullable: true)],
            [['tags' => ['a', 'b']], ['tags' => []], ['tags' => null]],
            "<row><tags><element>a</element><element>b</element></tags></row>\n<row><tags/></row>\n<row><tags></tags></row>\n",
        ];
        yield 'B3 list<?integer>' => [
            [list_schema('l', type_list(type_optional(type_integer())))],
            [['l' => [1, null, 3]]],
            "<row><l><element>1</element><element></element><element>3</element></l></row>\n",
        ];
        yield 'B3 map<string, ?integer>' => [
            [map_schema('m', type_map(type_string(), type_optional(type_integer())))],
            [['m' => ['x' => 1, 'y' => null]]],
            "<row><m><element><key>x</key><value>1</value></element><element><key>y</key><value></value></element></m></row>\n",
        ];
        yield 'map' => [
            [map_schema('m', type_map(type_string(), type_integer()))],
            [['m' => ['x' => 1]], ['m' => []]],
            "<row><m><element><key>x</key><value>1</value></element></m></row>\n<row><m/></row>\n",
        ];
        yield 'a map keyed by integers' => [
            [map_schema('m', type_map(type_integer(), type_string()))],
            [['m' => [5 => 'a']]],
            "<row><m><element><key>5</key><value>a</value></element></m></row>\n",
        ];
        yield 'structure' => [
            [structure_schema('address', type_structure(['city' => type_string(), 'zip' => type_string()]))],
            [['address' => ['city' => 'Krakow', 'zip' => '31-021']]],
            "<row><address><city>Krakow</city><zip>31-021</zip></address></row>\n",
        ];
        yield 'nested leaves render by their type' => [
            [
                list_schema('days', type_list(type_date())),
                structure_schema('s', type_structure(['d' => type_datetime(), 'f' => type_float()])),
                list_schema('docs', type_list(type_json())),
            ],
            [[
                'days' => [new DateTimeImmutable('2024-08-01'), new DateTimeImmutable('2024-08-02')],
                's' => ['d' => new DateTimeImmutable('2024-08-01 10:00:00 UTC'), 'f' => 1.0],
                'docs' => ['{"a": 1}'],
            ]],
            '<row><days><element>2024-08-01</element><element>2024-08-02</element></days>'
                . '<s><d>2024-08-01T10:00:00.000000+00:00</d><f>1.0</f></s>'
                . "<docs><element>{\"a\": 1}</element></docs></row>\n",
        ];
        yield 'a prefixed structure element is an attribute of its node' => [
            [structure_schema('address', type_structure(['_id' => type_integer(), 'city' => type_string()]))],
            [['address' => ['_id' => 7, 'city' => 'Krakow']]],
            "<row><address id=\"7\"><city>Krakow</city></address></row>\n",
        ];
        yield 'no rows' => [[int_schema('id')], [], ''];
    }

    /**
     * @param list<Definition<mixed>> $definitions
     * @param list<array<string, mixed>> $rows
     */
    #[DataProvider('batches')]
    public function test_both_writers_encode_a_batch_to_the_same_bytes(
        array $definitions,
        array $rows,
        string $expected,
    ): void {
        $batch = array_to_rows($rows, schema(...$definitions));

        static::assertSame($expected, (new XMLEncoder(new StringXMLWriter()))->encode($batch));
        static::assertSame($expected, (new XMLEncoder(new DOMDocumentWriter()))->encode($batch));
    }

    public function test_encodes_datetime_and_date_with_the_configured_formats(): void
    {
        static::assertSame(
            "<row><at>2024-08-01</at><on>01/08/2024</on></row>\n",
            (new XMLEncoder(new StringXMLWriter(), dateTimeFormat: 'Y-m-d', dateFormat: 'd/m/Y'))->encode(array_to_rows(
                [['at' => new DateTimeImmutable('2024-08-01 10:00:00'), 'on' => new DateTimeImmutable('2024-08-01')]],
                schema(datetime_schema('at'), date_schema('on')),
            )),
        );
    }

    public function test_uses_the_configured_element_names_and_attribute_prefix(): void
    {
        static::assertSame(
            '<record id="1"><l><item>a</item></l><m><entry><k>x</k><v>1</v></entry></m></record>' . "\n",
            (new XMLEncoder(
                new StringXMLWriter(),
                attributePrefix: '@',
                listElementName: 'item',
                mapElementName: 'entry',
                mapElementKeyName: 'k',
                mapElementValueName: 'v',
                rowElementName: 'record',
            ))->encode(array_to_rows(
                [['@id' => 1, 'l' => ['a'], 'm' => ['x' => 1]]],
                schema(
                    int_schema('@id'),
                    list_schema('l', type_list(type_string())),
                    map_schema('m', type_map(type_string(), type_integer())),
                ),
            )),
        );
    }

    public function test_a_null_in_an_attribute_column_is_refused(): void
    {
        $this->expectException(CastingException::class);

        (new XMLEncoder(new StringXMLWriter()))->encode(array_to_rows([[
            '_id' => null,
        ]], schema(int_schema('_id', nullable: true))));
    }

    public function test_a_row_element_name_dom_refuses_is_refused_by_either_writer(): void
    {
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));
        $refused = 0;

        foreach ([new StringXMLWriter(), new DOMDocumentWriter()] as $writer) {
            try {
                (new XMLEncoder($writer, rowElementName: '1 row'))->encode($rows);
            } catch (DOMException) {
                $refused++;
            }
        }

        static::assertSame(2, $refused);
    }

    public function test_encoding_without_a_writer_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('XMLEncoder requires an XMLWriter to encode rows');

        (new XMLEncoder())->encode(array_to_rows([['id' => 1]], schema(int_schema('id'))));
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
}
