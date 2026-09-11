<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use DOMDocument;
use DOMNodeList;
use DOMXPath;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Function\ArrayExpand;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class ScalarFunctionTransformerTest extends FlowTestCase
{
    public function test_bind_adds_the_derived_column(): void
    {
        static::assertEquals(
            schema(int_schema('a'), str_schema('b')),
            (new ScalarFunctionTransformer('b', lit('x')))->bind(schema(int_schema('a')))->output,
        );
    }

    public function test_bind_declares_one_prefixed_nullable_column_per_unpacked_column(): void
    {
        static::assertEquals(
            schema(int_schema('id'), int_schema('data.a', nullable: true), str_schema('data.b', nullable: true)),
            (new ScalarFunctionTransformer(
                'data',
                new ArrayUnpack(lit(['a' => 1, 'b' => 'x']), schema(int_schema('a'), str_schema('b'))),
            ))->bind(schema(int_schema('id')))->output,
        );
    }

    public function test_bind_replaces_an_existing_column_with_the_derived_one(): void
    {
        static::assertEquals(
            schema(str_schema('a')),
            (new ScalarFunctionTransformer('a', lit('x')))->bind(schema(int_schema('a')))->output,
        );
    }

    public function test_expand_results(): void
    {
        static::assertEquals(
            [
                ['array' => 1],
                ['array' => 2],
                ['array' => 3],
            ],
            (new ScalarFunctionTransformer('array', new ArrayExpand(lit([1, 2, 3]), ArrayExpand\ArrayExpand::VALUES)))
                ->transform(rows(schema(), row([])), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_lit_expression_on_empty_rows(): void
    {
        static::assertEquals(
            [],
            (new ScalarFunctionTransformer('number', lit(1_000)))
                ->transform(rows(schema()), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_lit_expression_on_non_empty_rows(): void
    {
        static::assertEquals(
            [
                ['name' => 'Norbert', 'number' => 1],
            ],
            (new ScalarFunctionTransformer('number', lit(1)))
                ->transform(rows(schema(str_schema('name')), row(['name' => 'Norbert'])), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_plus_expression_on_an_empty_batch_that_declares_its_columns(): void
    {
        static::assertEquals(
            [],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(rows(schema(int_schema('num'), int_schema('num1'))), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_plus_expression_on_non_empty_rows(): void
    {
        static::assertEquals(
            [
                ['a' => 1, 'b' => 2, 'c' => 3],
            ],
            (new ScalarFunctionTransformer('c', ref('a')->plus(ref('b'))))
                ->transform(
                    rows(schema(int_schema('a'), int_schema('b')), row(['a' => 1, 'b' => 2])),
                    flow_context(config()),
                )
                ->toArray(),
        );
    }

    public function test_plus_expression_on_non_existing_rows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition for entry "num" not found.');

        static::assertEquals(
            [
                ['a' => 1, 'number' => 0],
            ],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(rows(schema(int_schema('a')), row(['a' => 1])), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_an_undeclared_payload_key_is_dropped(): void
    {
        static::assertEquals(
            [
                ['array.id' => 1],
            ],
            (new ScalarFunctionTransformer(
                'array',
                new ArrayUnpack(lit(['id' => 1, 'secret' => 'dropped']), schema(int_schema('id'))),
            ))
                ->transform(rows(schema(), row([])), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_unpack_results_ignores_a_definition_passed_to_with_entry(): void
    {
        // a Definition passed to withEntry() cannot describe an N-column result, so unpack's own
        // declared Schema is what names and types the produced columns
        $result = (new ScalarFunctionTransformer(
            str_schema('array'),
            new ArrayUnpack(lit(['id' => 1, 'name' => 'Norbert']), schema(int_schema('id'), str_schema('name'))),
        ))->transform(rows(schema(), row([])), flow_context(config()));

        static::assertEquals([['array.id' => 1, 'array.name' => 'Norbert']], $result->toArray());
        static::assertEquals(
            schema(int_schema('array.id', nullable: true), str_schema('array.name', nullable: true)),
            $result->schema(),
        );
    }

    public function test_xml_xpath_expression_when_there_is_more_than_one_node_under_given_path(): void
    {
        $xml = '<root><foo baz="buz">bar</foo><foo>baz</foo></root>';
        $document = new DOMDocument();
        $document->loadXML($xml);
        $xpath = new DOMXPath($document);

        $nodes = type_instance_of(DOMNodeList::class)->assert($xpath->query('/root/foo'));
        $expected = [$nodes->item(0), $nodes->item(1)];

        $result = (new ScalarFunctionTransformer('xpath', ref('xml')->xpath('/root/foo')))->transform(
            rows(schema(xml_schema('xml')), row(['xml' => type_xml()->cast($xml)])),
            flow_context(config()),
        );

        static::assertEquals($expected, $result->first()->get('xpath'));
        static::assertEquals(type_list(type_xml_element()), $result->schema()->get('xpath')->type());
        static::assertTrue($result->schema()->get('xpath')->isNullable());
    }

    public function test_a_declared_output_definition_coerces_the_produced_value(): void
    {
        $result = (new ScalarFunctionTransformer(str_schema('out'), ref('v')))->transform(
            rows(schema(int_schema('v')), row(['v' => 1]), row(['v' => 2])),
            flow_context(config()),
        );

        static::assertSame(
            [
                ['v' => 1, 'out' => '1'],
                ['v' => 2, 'out' => '2'],
            ],
            $result->toArray(),
        );
    }

    public function test_the_produced_column_takes_the_declared_definition(): void
    {
        $result = (new ScalarFunctionTransformer(str_schema('out'), ref('v')))->transform(
            rows(schema(int_schema('v')), row(['v' => 1]), row(['v' => 2])),
            flow_context(config()),
        );

        static::assertEquals(type_string(), $result->schema()->get('out')->type());
    }

    public function test_the_produced_definition_does_not_depend_on_batch_size(): void
    {
        $wholeBatch = (new ScalarFunctionTransformer('out', ref('v')->plus(lit(1))))->transform(
            rows(schema(int_schema('v')), row(['v' => 1]), row(['v' => 2])),
            flow_context(config()),
        );

        $transformer = new ScalarFunctionTransformer('out', ref('v')->plus(lit(1)));
        $single = $transformer
            ->transform(rows(schema(int_schema('v')), row(['v' => 1])), flow_context(config()))
            ->merge($transformer->transform(rows(schema(int_schema('v')), row(['v' => 2])), flow_context(config())));

        static::assertEquals($wholeBatch->schema()->get('out'), $single->schema()->get('out'));
        static::assertEquals($wholeBatch->toArray(), $single->toArray());
    }

    public function test_a_null_produced_under_a_not_null_declaration_names_its_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "out" (row 1)');

        (new ScalarFunctionTransformer(int_schema('out'), ref('v')))->transform(
            rows(schema(int_schema('v', nullable: true)), row(['v' => 1]), row(['v' => null])),
            flow_context(config()),
        );
    }

    public function test_a_batch_arriving_under_another_schema_than_the_bound_one_is_conformed_to_the_bound_output(): void
    {
        $step = type_instance_of(ScalarFunctionTransformer::class)->assert((new ScalarFunctionTransformer(
            'out',
            lit('x'),
        ))->bind(schema(int_schema('id'), str_schema('name', nullable: true)))->step);

        static::assertSame(
            [['id' => 1, 'name' => null, 'out' => 'x']],
            $step->transform(rows(schema(int_schema('id')), row(['id' => 1])), flow_context(config()))->toArray(),
        );
    }

    public function test_an_expanded_null_under_a_not_null_declaration_names_its_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "out" (row 1)');

        (new ScalarFunctionTransformer(
            int_schema('out'),
            new ArrayExpand(lit([1, null]), ArrayExpand\ArrayExpand::VALUES),
        ))->transform(rows(schema(), row([])), flow_context(config()));
    }

    public function test_an_empty_batch_is_bound_against_its_own_schema(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new ScalarFunctionTransformer('out', ref('missing')->upper()))->transform(
            rows(schema()),
            flow_context(config()),
        );
    }
}
