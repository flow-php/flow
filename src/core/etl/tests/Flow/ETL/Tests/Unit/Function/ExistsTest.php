<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Exists;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\exists;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function in_array;
use function range;

final class ExistsTest extends FlowTestCase
{
    public function test_a_throwing_operand_means_the_reference_does_not_exist(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Exists(ref('value')->upper()),
            [
                'value' => 1,
            ],
            schema(int_schema('value')),
        ));
    }

    public function test_if_reference_exists(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('value')->exists(),
            ['value' => 'test'],
            schema(str_schema('value')),
        ));
    }

    public function test_that_lit_function_exists(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(new Exists(lit('val')), [], schema()));
    }

    public function test_that_null_reference_to_null_entry_exists(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('value')->exists(),
            ['value' => null],
            schema(str_schema('value', nullable: true)),
        ));
    }

    public function test_that_reference_does_not_exists(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(ref('value')->exists(), [], schema()));
    }

    /**
     * @return Generator<string, array{list<int>, string, int}>
     */
    public static function failing_rows(): Generator
    {
        yield 'one failing row: one retry' => [[3], FailingOnValuesFunction::WITH_ROW, 2];
        yield 'three failing rows: three retries' => [[2, 4, 6], FailingOnValuesFunction::WITH_ROW, 4];
        yield 'twelve failing rows: eight batch evals, then each remaining row alone' => [
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            FailingOnValuesFunction::WITH_ROW,
            8 + 6,
        ];
        yield 'a failure without a row: each row alone' => [[3], FailingOnValuesFunction::WITHOUT_ROW, 1 + 14];
    }

    /**
     * @param list<int> $failing
     */
    #[DataProvider('failing_rows')]
    public function test_only_the_failing_rows_are_false(array $failing, string $failure, int $evals): void
    {
        $spy = new FailingOnValuesFunction(ref('v'), $failing, $failure);
        $rows = array_to_rows(
            array_map(static fn(int $v): array => ['v' => $v], range(1, 14)),
            schema(int_schema('v')),
        );

        $values = exists($spy)->eval($rows, flow_context())->values();

        static::assertSame(array_map(static fn(int $v): bool => !in_array($v, $failing, true), range(1, 14)), $values);
        static::assertSame($evals, $spy->evals);
    }

    public function test_a_reference_is_a_schema_fact(): void
    {
        $rows = array_to_rows([['v' => 1], ['v' => 2]], schema(int_schema('v')));

        static::assertSame([true, true], exists(ref('v'))->eval($rows, flow_context())->values());
        static::assertSame([false, false], exists(ref('missing'))->eval($rows, flow_context())->values());
    }
}
