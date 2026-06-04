<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\With;

use Flow\PostgreSql\QueryBuilder\Select\SelectFromStep;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\cte;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\with;

final class WithBuilderTest extends TestCase
{
    public function test_select_returns_from_step_and_renders_with_clause(): void
    {
        $builder = with(cte('t', select(col('id'))->from(table('s'))))->select(col('id'));

        static::assertInstanceOf(SelectFromStep::class, $builder);
        static::assertSame('WITH t AS (SELECT id FROM s) SELECT id FROM t', $builder->from(table('t'))->toSql());
    }

    public function test_select_distinct_returns_from_step_and_renders_with_clause(): void
    {
        $builder = with(cte('t', select(col('id'))->from(table('s'))))->selectDistinct(col('id'));

        static::assertInstanceOf(SelectFromStep::class, $builder);
        static::assertSame(
            'WITH t AS (SELECT id FROM s) SELECT DISTINCT id FROM t',
            $builder->from(table('t'))->toSql(),
        );
    }

    public function test_select_distinct_on_returns_from_step_and_renders_with_clause(): void
    {
        $builder = with(cte('t', select(col('id'), col('v'))->from(table('s'))))
            ->selectDistinctOn([col('id')], col('id'), col('v'));

        static::assertInstanceOf(SelectFromStep::class, $builder);
        static::assertSame(
            'WITH t AS (SELECT id, v FROM s) SELECT DISTINCT ON (id) id, v FROM t',
            $builder->from(table('t'))->toSql(),
        );
    }
}
