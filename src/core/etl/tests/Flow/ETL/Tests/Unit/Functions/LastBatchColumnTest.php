<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\LastBatchColumn;
use Flow\ETL\Tests\FlowTestCase;
use WeakReference;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function gc_collect_cycles;

final class LastBatchColumnTest extends FlowTestCase
{
    public function test_the_same_batch_resolves_its_column_once(): void
    {
        $column = new LastBatchColumn('id');
        $rows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));

        static::assertSame($column->of($rows), $column->of($rows));
    }

    public function test_another_batch_resolves_its_own_column(): void
    {
        $column = new LastBatchColumn('id');

        static::assertSame([1], $column->of(array_to_rows([['id' => 1]], schema(int_schema('id'))))?->values());
        static::assertSame([2], $column->of(array_to_rows([['id' => 2]], schema(int_schema('id'))))?->values());
    }

    public function test_a_batch_without_the_column_has_none(): void
    {
        static::assertNull((new LastBatchColumn('missing'))->of(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));
    }

    public function test_the_last_batch_is_not_kept_alive(): void
    {
        $column = new LastBatchColumn('id');
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')));
        $column->of($rows);
        $reference = WeakReference::create($rows);

        unset($rows);
        gc_collect_cycles();

        static::assertNull($reference->get());
    }
}
