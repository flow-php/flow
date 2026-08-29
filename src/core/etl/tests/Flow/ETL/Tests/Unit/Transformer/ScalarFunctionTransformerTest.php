<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use DOMDocument;
use DOMNodeList;
use DOMXPath;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayExpand;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_xml_element;

final class ScalarFunctionTransformerTest extends FlowTestCase
{
    public function test_expand_results(): void
    {
        static::assertEquals(
            [
                ['array' => 1],
                ['array' => 2],
                ['array' => 3],
            ],
            (new ScalarFunctionTransformer('array', new ArrayExpand(lit([1, 2, 3]), ArrayExpand\ArrayExpand::VALUES)))
                ->transform(rows(row()), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_lit_expression_on_empty_rows(): void
    {
        static::assertEquals(
            [],
            (new ScalarFunctionTransformer('number', lit(1_000)))
                ->transform(rows(), flow_context(config()))
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
                ->transform(rows(row(str_entry('name', 'Norbert'))), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_plus_expression_on_empty_rows(): void
    {
        static::assertEquals(
            [],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(rows(), flow_context(config()))
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
                ->transform(rows(row(int_entry('a', 1), int_entry('b', 2))), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_plus_expression_on_non_existing_rows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition for entry "num" not found. Available columns: [a].');

        static::assertEquals(
            [
                ['a' => 1, 'number' => 0],
            ],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(rows(row(int_entry('a', 1))), flow_context(config()))
                ->toArray(),
        );
    }

    public function test_unpack_results(): void
    {
        static::assertEquals(
            [
                ['array.id' => 1, 'array.name' => 'Norbert'],
            ],
            (new ScalarFunctionTransformer('array', new ArrayUnpack(lit(['id' => 1, 'name' => 'Norbert']))))
                ->transform(rows(row()), flow_context(config()))
                ->toArray(),
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

        $entry = (new ScalarFunctionTransformer('xpath', ref('xml')->xpath('/root/foo')))
            ->transform(rows(row(xml_entry('xml', $xml))), flow_context(config()))
            ->first()
            ->get(ref('xpath'));

        static::assertEquals($expected, $entry->value());
        static::assertEquals(type_list(type_xml_element()), $entry->definition()->type());
        static::assertTrue($entry->definition()->isNullable());
    }

    public function test_a_widened_batch_definition_coerces_the_value(): void
    {
        $result = (new ScalarFunctionTransformer('out', ref('v')))->transform(
            rows(row(int_entry('v', 1)), row(str_entry('v', 'a'))),
            flow_context(config()),
        );

        static::assertSame(
            [
                ['v' => 1, 'out' => '1'],
                ['v' => 'a', 'out' => 'a'],
            ],
            $result->toArray(),
        );
    }

    public function test_a_heterogeneous_batch_yields_one_definition_for_the_produced_column(): void
    {
        $result = (new ScalarFunctionTransformer('out', ref('v')))->transform(
            rows(row(int_entry('v', 1)), row(str_entry('v', 'a'))),
            flow_context(config()),
        );

        $definitions = [];

        foreach ($result as $row) {
            $definitions[] = $row->get('out')->definition();
        }

        static::assertCount(2, $definitions);
        static::assertEquals($definitions[0], $definitions[1]);
    }

    public function test_the_produced_definition_does_not_depend_on_batch_size(): void
    {
        $wholeBatch = (new ScalarFunctionTransformer('out', ref('v')->plus(lit(1))))->transform(
            rows(row(int_entry('v', 1)), row(int_entry('v', 2))),
            flow_context(config()),
        );

        $transformer = new ScalarFunctionTransformer('out', ref('v')->plus(lit(1)));
        $single = $transformer
            ->transform(rows(row(int_entry('v', 1))), flow_context(config()))
            ->merge($transformer->transform(rows(row(int_entry('v', 2))), flow_context(config())));

        static::assertEquals(
            $wholeBatch->first()->get('out')->definition(),
            $single->first()->get('out')->definition(),
        );
        static::assertEquals($wholeBatch->toArray(), $single->toArray());
    }

    public function test_an_empty_batch_is_returned_without_binding(): void
    {
        $empty = rows();

        static::assertSame($empty, (new ScalarFunctionTransformer('out', ref('missing')->upper()))->transform(
            $empty,
            flow_context(config()),
        ));
    }
}
