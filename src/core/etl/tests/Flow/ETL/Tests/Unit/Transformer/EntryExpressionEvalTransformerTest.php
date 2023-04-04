<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use PHPUnit\Framework\TestCase;

final class EntryExpressionEvalTransformerTest extends TestCase
{
    public function test_lit_expression_on_empty_rows() : void
    {
        $this->assertEquals(
            [
            ],
            (new EntryExpressionEvalTransformer('number', lit(1_000)))
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
            (new EntryExpressionEvalTransformer('number', lit(1)))
                ->transform(
                    new Rows(Row::create(Entry::string('name', 'Norbert'))),
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
            (new EntryExpressionEvalTransformer('number', ref('num')->plus(ref('num1'))))
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
            (new EntryExpressionEvalTransformer('c', ref('a')->plus(ref('b'))))
                ->transform(new Rows(
                    Row::create(Entry::integer('a', 1), Entry::integer('b', 2))
                ), new FlowContext(Config::default()))
                ->toArray()
        );
    }

    public function test_plus_expression_on_non_existing_rows() : void
    {
        $this->assertEquals(
            [
                ['a' => 1, 'number' => 0],
            ],
            (new EntryExpressionEvalTransformer('number', ref('num')->plus(ref('num1'))))
                ->transform(
                    new Rows(Row::create(Entry::integer('a', 1))),
                    new FlowContext(Config::default())
                )
                ->toArray()
        );
    }
}
