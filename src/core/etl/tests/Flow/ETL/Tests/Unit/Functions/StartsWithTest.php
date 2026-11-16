<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StartsWithTest extends FlowTestCase
{
    public function test_a_column_needle(): void
    {
        static::assertSame(
            [false, null, null, true, true, false, false],
            (new FunctionContext(flow_context()))
                ->column(
                    ref('h')->startsWith(ref('n')),
                    [
                        ['h' => 'abcx', 'n' => 'x'],
                        ['h' => null, 'n' => 'x'],
                        ['h' => 'abc', 'n' => null],
                        ['h' => 'abc', 'n' => ''],
                        ['h' => '', 'n' => ''],
                        ['h' => 'żółw', 'n' => 'w'],
                        ['h' => 'x', 'n' => 'xx'],
                    ],
                    schema(str_schema('h', nullable: true), str_schema('n', nullable: true)),
                )
                ->values(),
        );
    }

    public function test_a_literal_needle(): void
    {
        static::assertSame(
            [false, null, false, false, false, false, true],
            (new FunctionContext(flow_context()))
                ->column(
                    ref('h')->startsWith(lit('x')),
                    [
                        ['h' => 'abcx', 'n' => 'x'],
                        ['h' => null, 'n' => 'x'],
                        ['h' => 'abc', 'n' => null],
                        ['h' => 'abc', 'n' => ''],
                        ['h' => '', 'n' => ''],
                        ['h' => 'żółw', 'n' => 'w'],
                        ['h' => 'x', 'n' => 'xx'],
                    ],
                    schema(str_schema('h', nullable: true), str_schema('n', nullable: true)),
                )
                ->values(),
        );
    }

    public function test_the_result_is_a_boolean_column(): void
    {
        $column = (new FunctionContext(flow_context()))->column(
            ref('h')->startsWith(ref('n')),
            [
                ['h' => 'abcx', 'n' => 'x'],
                ['h' => null, 'n' => 'x'],
                ['h' => 'abc', 'n' => null],
                ['h' => 'abc', 'n' => ''],
            ],
            schema(str_schema('h', nullable: true), str_schema('n', nullable: true)),
        );

        static::assertSame('boolean', $column->type()->toString());
        static::assertSame(2, $column->nullCount());
    }

    public function test_an_empty_batch_gives_an_empty_column(): void
    {
        static::assertSame(
            [],
            (new FunctionContext(flow_context()))
                ->column(ref('h')->startsWith(lit('x')), [], schema(str_schema('h', nullable: true)))
                ->values(),
        );
    }

    public function test_a_haystack_that_is_not_a_string_is_refused_with_its_row(): void
    {
        try {
            (new FunctionContext(flow_context()))->column(
                ref('i')->startsWith(lit('1')),
                [['i' => null], ['i' => 12]],
                schema(int_schema('i', nullable: true)),
            );
            static::fail('an integer haystack was matched');
        } catch (EvaluationException $e) {
            static::assertSame(1, $e->rowIndex);
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
            static::assertSame('Expected type "string", got "integer".', $e->getPrevious()->getMessage());
        }
    }

    public function test_a_needle_that_is_not_a_string_is_refused_with_its_row(): void
    {
        try {
            (new FunctionContext(flow_context()))->column(
                ref('h')->startsWith(ref('i')),
                [['h' => 'a1', 'i' => 1]],
                schema(str_schema('h'), int_schema('i')),
            );
            static::fail('an integer needle was matched');
        } catch (EvaluationException $e) {
            static::assertSame(0, $e->rowIndex);
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
            static::assertSame('Expected type "string", got "integer".', $e->getPrevious()->getMessage());
        }
    }

    public function test_string_columns_are_matched_by_the_backend_without_a_builder(): void
    {
        $schema = schema(str_schema('h'), str_schema('n'));
        $backend = new SpyBackend();
        (new ReferenceResolver())
            ->resolve(ref('h')->startsWith(ref('n')), $schema)
            ->eval(array_to_rows([['h' => 'ax', 'n' => 'x']], $schema, new PhpBackend()), $backend);

        static::assertSame(0, $backend->builders());
    }
}
