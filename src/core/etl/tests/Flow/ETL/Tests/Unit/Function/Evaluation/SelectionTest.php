<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Evaluation;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Function\Evaluation\Selection;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SelectionTest extends FlowTestCase
{
    public function test_all_rows_evaluate_the_batch_itself(): void
    {
        $rows = array_to_rows(
            [['v' => 1, 'name' => 'a'], ['v' => 2, 'name' => 'b']],
            schema(int_schema('v'), str_schema('name')),
        );
        $spy = new FailingOnValuesFunction(ref('v'));

        $column = (new Selection([0, 1]))->evaluate(
            (new ReferenceResolver())->resolve($spy, $rows->schema()),
            $rows,
            flow_context(),
        );

        static::assertSame([1, 2], $column->values());
        static::assertSame($rows, $spy->seen[0]);
    }

    public function test_an_error_names_the_row_of_the_batch(): void
    {
        $rows = array_to_rows([['v' => 1], ['v' => 2], ['v' => 3], ['v' => 4]], schema(int_schema('v')));

        try {
            (new Selection([1, 3]))->evaluate(
                (new ReferenceResolver())->resolve(new FailingOnValuesFunction(ref('v'), [4]), $rows->schema()),
                $rows,
                flow_context(),
            );
            static::fail('expected an EvaluationException');
        } catch (EvaluationException $e) {
            static::assertSame(3, $e->rowIndex);
            static::assertSame('fails on 4 (row 3)', $e->getMessage());
        }
    }

    public function test_a_subset_holds_only_the_selected_rows_of_the_referenced_columns(): void
    {
        $rows = array_to_rows(
            [['v' => 1, 'name' => 'a'], ['v' => 2, 'name' => 'b'], ['v' => 3, 'name' => 'c']],
            schema(int_schema('v'), str_schema('name')),
        );
        $spy = new FailingOnValuesFunction(ref('v'));

        $column = (new Selection([0, 2]))->evaluate(
            (new ReferenceResolver())->resolve($spy, $rows->schema()),
            $rows,
            flow_context(),
        );

        static::assertSame([1, 3], $column->values());
        static::assertSame(['v'], array_keys($spy->seen[0]->columns()));
        static::assertSame(2, $spy->seen[0]->count());
    }
}
