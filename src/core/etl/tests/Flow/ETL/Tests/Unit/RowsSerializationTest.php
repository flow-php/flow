<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DOMDocument;
use DOMElement;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\Physical\HtmlDocumentPhysical;
use Flow\ETL\Column\Physical\HtmlElementPhysical;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Context\RowsSerializationContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\Exception\FloeException;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function assert;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function pack;
use function serialize;
use function unserialize;

final class RowsSerializationTest extends FlowTestCase
{
    public function test_native_columns_survive_a_serialize_round_trip(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        /** @var Rows $restored */
        $restored = unserialize(serialize($rows));

        static::assertSame([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => null]], $restored->toArray());
        static::assertTrue($rows->schema()->isSame($restored->schema()));
    }

    public function test_xml_column_metadata_survives_a_serialize_round_trip(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a>1</a>');

        $rows = array_to_rows([[
            'x' => $document,
        ]], schema(xml_schema('x', metadata: Metadata::empty()->add('src', 'file.xml'))));

        /** @var Rows $restored */
        $restored = unserialize(serialize($rows));
        // @mago-ignore analysis:mixed-assignment
        $restoredDocument = $restored->column('x')->value(0);
        assert($restoredDocument instanceof DOMDocument);

        static::assertSame(['src' => 'file.xml'], $restored->schema()->get('x')->metadata()->normalize());
        static::assertSame($rows->schema()->get('x')->isNullable(), $restored->schema()->get('x')->isNullable());
        static::assertSame($document->C14N(), $restoredDocument->C14N());
    }

    public function test_xml_document_survives_a_round_trip(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a><b>1</b></a>');

        $restored = type_instance_of(DOMDocument::class)->assert(
            RowsSerializationContext::roundTrip(array_to_rows([[
                'x' => $document,
            ]], schema(xml_schema('x'))))
                ->column('x')
                ->value(0),
        );

        static::assertSame($document->C14N(), $restored->C14N());
    }

    public function test_xml_element_survives_a_round_trip(): void
    {
        $document = new DOMDocument();
        $document->loadXML('<a><b>1</b></a>');

        $restored = type_instance_of(DOMElement::class)->assert(
            RowsSerializationContext::roundTrip(array_to_rows([[
                'x' => $document->documentElement,
            ]], schema(xml_element_schema('x'))))
                ->column('x')
                ->value(0),
        );

        static::assertSame('<a><b>1</b></a>', $restored->C14N());
    }

    public function test_restored_xml_elements_do_not_canonicalize_to_an_empty_string(): void
    {
        $left = new DOMDocument();
        $left->loadXML('<a><b>1</b></a>');
        $right = new DOMDocument();
        $right->loadXML('<z><y>999</y></z>');

        $restored = RowsSerializationContext::roundTrip(array_to_rows([
            ['x' => $left->documentElement],
            ['x' => $right->documentElement],
        ], schema(xml_element_schema('x'))));
        $restoredLeft = type_instance_of(DOMElement::class)->assert($restored->column('x')->value(0));
        $restoredRight = type_instance_of(DOMElement::class)->assert($restored->column('x')->value(1));

        static::assertNotSame('', $restoredLeft->C14N());
        static::assertNotSame($restoredLeft->C14N(), $restoredRight->C14N());
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_document_survives_a_round_trip(): void
    {
        $document = type_object()->assert((new HtmlDocumentPhysical())->fromPhysical('<p>x</p>'));

        static::assertSame(
            type_string()->cast($document),
            type_string()->cast(
                RowsSerializationContext::roundTrip(array_to_rows([[
                    'x' => $document,
                ]], schema(html_schema('x'))))
                    ->column('x')
                    ->value(0),
            ),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_html_element_survives_a_round_trip(): void
    {
        $element = type_object()->assert((new HtmlElementPhysical())->fromPhysical('<p>x</p>'));

        static::assertSame(
            type_string()->cast($element),
            type_string()->cast(
                RowsSerializationContext::roundTrip(array_to_rows([[
                    'x' => $element,
                ]], schema(html_element_schema('x'))))
                    ->column('x')
                    ->value(0),
            ),
        );
    }

    public function test_container_columns_survive_a_round_trip(): void
    {
        $rows = array_to_rows(
            [
                ['l' => [1, null], 'm' => ['a' => 1], 's' => ['id' => 1, 'note' => 'x']],
                ['l' => null, 'm' => [], 's' => ['id' => 2]],
            ],
            schema(
                list_schema('l', type_list(type_optional(type_integer())), nullable: true),
                map_schema('m', type_map(type_string(), type_integer())),
                structure_schema('s', type_structure([
                    'id' => type_integer(),
                    'note' => structure_element('note', type_string(), optional: true),
                ])),
            ),
        );

        static::assertEquals($rows, RowsSerializationContext::roundTrip($rows));
    }

    public function test_constant_columns_survive_a_round_trip(): void
    {
        $schema = schema(int_schema('id'), str_schema('tag', nullable: true));
        $backend = new PhpBackend();
        $rows = Rows::fromColumns(
            $schema,
            [
                'id' => $backend->constant(int_schema('id'), 7, 3),
                'tag' => $backend->constant(str_schema('tag', nullable: true), null, 3),
            ],
            3,
        );

        static::assertSame(
            [['id' => 7, 'tag' => null], ['id' => 7, 'tag' => null], ['id' => 7, 'tag' => null]],
            RowsSerializationContext::roundTrip($rows)->toArray(),
        );
    }

    public function test_a_batch_without_columns_keeps_its_row_count(): void
    {
        $rows = array_to_rows([[], [], []], schema());

        static::assertSame(3, RowsSerializationContext::roundTrip($rows)->count());
    }

    public function test_unserialize_refuses_a_frame_that_disagrees_with_the_schema(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe BATCH frame holds 2 nodes, the schema describes 1');

        RowsSerializationContext::unserialize([
            'schema' => schema(int_schema('id')),
            'frames' => [array_to_rows(
                [['id' => 1, 'other' => 2]],
                schema(int_schema('id'), int_schema('other')),
            )->encodeFrame()],
        ]);
    }

    public function test_unserialize_refuses_a_corrupt_buffer(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('is malformed: Int64 values buffer of 2 bytes, expected 8 for 1 rows');

        RowsSerializationContext::unserialize([
            'schema' => schema(int_schema('id')),
            'frames' => [
                pack('VVV', 1, 1, 2)
                    . pack('VV', 1, 0)
                    . pack('VV', 0, 0)
                    . pack('VV', 0, 10)
                    . "\0\0\0\0"
                    . pack('P', -1)
                    . "\x01\x00\0\0\0\0\0\0",
            ],
        ]);
    }
}
