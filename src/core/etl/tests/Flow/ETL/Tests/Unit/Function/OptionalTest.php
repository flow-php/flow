<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Error;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function in_array;
use function range;

final class OptionalTest extends FlowTestCase
{
    public function test_optional_declares_the_inner_type_widened_to_optional(): void
    {
        static::assertSame('?string', optional(ref('name')->upper())->returns()->toString());
    }

    public function test_optional_returns_null_when_the_inner_function_throws(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            optional(ref('name')->upper()),
            ['other' => 'flow'],
            schema(str_schema('other')),
        ));
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
    public function test_only_the_failing_rows_are_null(array $failing, string $failure, int $evals): void
    {
        $spy = new FailingOnValuesFunction(ref('v'), $failing, $failure);
        $rows = array_to_rows(
            array_map(static fn(int $v): array => ['v' => $v], range(1, 14)),
            schema(int_schema('v')),
        );

        $values = (new ReferenceResolver())
            ->resolve(optional($spy), $rows->schema())
            ->eval($rows, flow_context())
            ->values();

        static::assertSame(
            array_map(static fn(int $v): ?int => in_array($v, $failing, true) ? null : $v, range(1, 14)),
            $values,
        );
        static::assertSame($evals, $spy->evals);
    }

    public function test_an_error_propagates(): void
    {
        $rows = array_to_rows([['v' => 1]], schema(int_schema('v')));

        $this->expectException(Error::class);

        (new ReferenceResolver())
            ->resolve(
                optional(new FailingOnValuesFunction(ref('v'), [1], FailingOnValuesFunction::ERROR)),
                $rows->schema(),
            )
            ->eval($rows, flow_context());
    }
}
