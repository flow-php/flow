<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, flow_context, row, rows};
use function Flow\ETL\DSL\{int_entry, list_entry, lit, ref, str_entry, xml_entry};
use function Flow\Types\DSL\{type_list, type_xml_element};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Function\ArrayExpand, Function\ArrayUnpack, Tests\FlowTestCase};
use Flow\ETL\Transformer\ScalarFunctionTransformer;

final class ScalarFunctionTransformerTest extends FlowTestCase
{
    public function test_expand_results() : void
    {
        self::assertEquals(
            [
                ['array' => 1],
                ['array' => 2],
                ['array' => 3],
            ],
            (new ScalarFunctionTransformer('array', new ArrayExpand(lit([1, 2, 3]), ArrayExpand\ArrayExpand::VALUES)))
                ->transform(rows(row()), flow_context(config()))
                ->toArray()
        );
    }

    public function test_lit_expression_on_empty_rows() : void
    {
        self::assertEquals(
            [
            ],
            (new ScalarFunctionTransformer('number', lit(1_000)))
                ->transform(rows(), flow_context(config()))
                ->toArray()
        );
    }

    public function test_lit_expression_on_non_empty_rows() : void
    {
        self::assertEquals(
            [
                ['name' => 'Norbert', 'number' => 1],
            ],
            (new ScalarFunctionTransformer('number', lit(1)))
                ->transform(
                    rows(row(str_entry('name', 'Norbert'))),
                    flow_context(config())
                )
                ->toArray()
        );
    }

    public function test_plus_expression_on_empty_rows() : void
    {
        self::assertEquals(
            [
            ],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(rows(), flow_context(config()))
                ->toArray()
        );
    }

    public function test_plus_expression_on_non_empty_rows() : void
    {
        self::assertEquals(
            [
                ['a' => 1, 'b' => 2, 'c' => 3],
            ],
            (new ScalarFunctionTransformer('c', ref('a')->plus(ref('b'))))
                ->transform(rows(row(int_entry('a', 1), int_entry('b', 2))), flow_context(config()))
                ->toArray()
        );
    }

    public function test_plus_expression_on_non_existing_rows() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "num" does not exist. Did you mean one of the following? ["a"]');

        self::assertEquals(
            [
                ['a' => 1, 'number' => 0],
            ],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(
                    rows(row(int_entry('a', 1))),
                    flow_context(config())
                )
                ->toArray()
        );
    }

    public function test_unpack_results() : void
    {
        self::assertEquals(
            [
                ['array.id' => 1, 'array.name' => 'Norbert'],
            ],
            (new ScalarFunctionTransformer('array', new ArrayUnpack(lit(['id' => 1, 'name' => 'Norbert']))))
                ->transform(rows(row()), flow_context(config()))
                ->toArray()
        );
    }

    public function test_xml_xpath_expression_when_there_is_more_than_one_node_under_given_path() : void
    {
        $xml = '<root><foo baz="buz">bar</foo><foo>baz</foo></root>';
        $document = new \DOMDocument();
        $document->loadXML($xml);
        $xpath = new \DOMXPath($document);

        $nodes = $xpath->query('/root/foo');
        $expected = $nodes ? [$nodes->item(0), $nodes->item(1)] : null;

        self::assertEquals(
            list_entry('xpath', $expected, type_list(type_xml_element())),
            (new ScalarFunctionTransformer('xpath', ref('xml')->xpath('/root/foo')))
                ->transform(
                    rows(row(xml_entry('xml', $xml))),
                    flow_context(config())
                )
                ->first()
                ->get(ref('xpath'))
        );
    }
}
