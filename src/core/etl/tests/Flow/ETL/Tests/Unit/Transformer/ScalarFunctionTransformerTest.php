<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_xml_node;
use function Flow\ETL\DSL\xml_entry;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use PHPUnit\Framework\TestCase;

final class ScalarFunctionTransformerTest extends TestCase
{
    public function test_lit_expression_on_empty_rows() : void
    {
        $this->assertEquals(
            [
            ],
            (new ScalarFunctionTransformer('number', lit(1_000)))
                ->transform(new Rows(), new FlowContext(Config::default()))
                ->toArray()
        );
    }

    public function test_lit_expression_on_non_empty_rows() : void
    {
        $this->assertEquals(
            [
                ['name' => 'Norbert', 'number' => 1],
            ],
            (new ScalarFunctionTransformer('number', lit(1)))
                ->transform(
                    new Rows(Row::create(str_entry('name', 'Norbert'))),
                    new FlowContext(Config::default())
                )
                ->toArray()
        );
    }

    public function test_plus_expression_on_empty_rows() : void
    {
        $this->assertEquals(
            [
            ],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(new Rows(), new FlowContext(Config::default()))
                ->toArray()
        );
    }

    public function test_plus_expression_on_non_empty_rows() : void
    {
        $this->assertEquals(
            [
                ['a' => 1, 'b' => 2, 'c' => 3],
            ],
            (new ScalarFunctionTransformer('c', ref('a')->plus(ref('b'))))
                ->transform(new Rows(
                    Row::create(int_entry('a', 1), int_entry('b', 2))
                ), new FlowContext(Config::default()))
                ->toArray()
        );
    }

    public function test_plus_expression_on_non_existing_rows() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry "num" does not exist. Did you mean one of the following? ["a"]');

        $this->assertEquals(
            [
                ['a' => 1, 'number' => 0],
            ],
            (new ScalarFunctionTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(
                    new Rows(Row::create(int_entry('a', 1))),
                    new FlowContext(Config::default())
                )
                ->toArray()
        );
    }

    public function test_xml_xpath_expression_when_there_is_more_than_one_node_under_given_path() : void
    {
        $xml = '<root><foo baz="buz">bar</foo><foo>baz</foo></root>';
        $document = new \DOMDocument();
        $document->loadXML($xml);
        $xpath = new \DOMXPath($document);

        $this->assertEquals(
            list_entry('xpath', [
                $xpath->query('/root/foo')->item(0),
                $xpath->query('/root/foo')->item(1),
            ], type_list(type_xml_node())),
            (new ScalarFunctionTransformer('xpath', ref('xml')->xpath('/root/foo')))
                ->transform(
                    new Rows(Row::create(xml_entry('xml', $xml))),
                    new FlowContext(Config::default())
                )
                ->first()
                ->get(ref('xpath'))
        );
    }
}
