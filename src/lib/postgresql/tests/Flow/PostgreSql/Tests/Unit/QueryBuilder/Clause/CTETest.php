<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Clause\CTE;
use Flow\PostgreSql\QueryBuilder\Clause\CTEMaterialization;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\cte;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\merge;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\update;
use function Flow\PostgreSql\DSL\with;

final class CTETest extends TestCase
{
    public function test_converts_simple_cte_to_ast(): void
    {
        $query = (new Node())->setSelectStmt(select(star())->from(table('users'))->toAst());
        $cte = new CTE('active_users', $query);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        static::assertNotNull($commonTableExpr);
        static::assertSame('active_users', $commonTableExpr->getCtename());
        static::assertSame($query, $commonTableExpr->getCtequery());
    }

    public function test_converts_to_ast_with_column_names(): void
    {
        $cte = cte('totals', select(star())->from(table('users')), ['category', 'amount']);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        static::assertNotNull($commonTableExpr);

        $aliasColumns = $commonTableExpr->getAliascolnames();
        static::assertNotNull($aliasColumns);
        static::assertCount(2, $aliasColumns);

        $firstColumn = $aliasColumns[0]->getString();
        static::assertNotNull($firstColumn);
        static::assertSame('category', $firstColumn->getSval());

        $secondColumn = $aliasColumns[1]->getString();
        static::assertNotNull($secondColumn);
        static::assertSame('amount', $secondColumn->getSval());
    }

    public function test_cte_getters(): void
    {
        $query = (new Node())->setSelectStmt(select(star())->from(table('users'))->toAst());
        $cte = new CTE('test_cte', $query, ['col1', 'col2'], CTEMaterialization::MATERIALIZED);

        static::assertSame('test_cte', $cte->name());
        static::assertSame($query, $cte->query());
        static::assertSame(['col1', 'col2'], $cte->columnNames());
        static::assertSame(CTEMaterialization::MATERIALIZED, $cte->materialization());
    }

    public function test_cte_with_delete_body(): void
    {
        $cte = cte(
            'd',
            delete()
                ->from('children')
                ->where(eq(col('parent_id'), literal(1)))
                ->returning(col('id')),
        );

        static::assertNotNull($cte->query()->getDeleteStmt());
        static::assertSame(
            'WITH d AS (DELETE FROM children WHERE parent_id = 1 RETURNING id) SELECT id FROM d',
            with($cte)->select(col('id'))->from(table('d'))->toSql(),
        );
    }

    public function test_cte_with_insert_body(): void
    {
        $cte = cte(
            'i',
            insert()->into('audit')->columns('id', 'note')->values(literal(1), literal('x'))->returning(col('id')),
        );

        static::assertNotNull($cte->query()->getInsertStmt());
        static::assertSame(
            "WITH i AS (INSERT INTO audit (id, note) VALUES (1, 'x') RETURNING id) SELECT id FROM i",
            with($cte)->select(col('id'))->from(table('i'))->toSql(),
        );
    }

    public function test_cte_with_merge_body(): void
    {
        $cte = cte(
            'm',
            merge('children', 'c')
                ->using('parents', 'p')
                ->on(eq(col('parent_id', 'c'), col('id', 'p')))
                ->whenMatched()
                ->thenDelete(),
        );

        static::assertNotNull($cte->query()->getMergeStmt());
        static::assertSame(
            'WITH m AS (MERGE INTO children c USING parents p ON c.parent_id = p.id WHEN MATCHED THEN DELETE) SELECT 1',
            with($cte)->select(literal(1))->toSql(),
        );
    }

    public function test_cte_with_select_body(): void
    {
        $cte = cte('s', select(col('id'))->from(table('parents')));

        static::assertNotNull($cte->query()->getSelectStmt());
        static::assertSame(
            'WITH s AS (SELECT id FROM parents) SELECT id FROM s',
            with($cte)->select(col('id'))->from(table('s'))->toSql(),
        );
    }

    public function test_cte_with_update_body(): void
    {
        $cte = cte('u', update()
            ->update('children')
            ->set('parent_id', literal(2))
            ->where(eq(col('id'), literal(10)))
            ->returning(col('id'), col('parent_id')));

        static::assertNotNull($cte->query()->getUpdateStmt());
        static::assertSame(
            'WITH u AS (UPDATE children SET parent_id = 2 WHERE id = 10 RETURNING id, parent_id) SELECT id, parent_id FROM u',
            with($cte)->select(col('id'), col('parent_id'))->from(table('u'))->toSql(),
        );
    }

    public function test_cycle_requires_at_least_one_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CYCLE needs at least one column');

        cte('t', select(star())->from(table('users')))->cycle([], 'is_cycle', 'path');
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_cycle_requires_both_mark_value_and_mark_default_or_neither(bool $withMarkValue): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CYCLE needs both a mark value and a mark default, or neither');

        cte('t', select(star())->from(table('users')))->cycle(
            ['id'],
            'is_cycle',
            'path',
            $withMarkValue ? literal('Y') : null,
            $withMarkValue ? null : literal('N'),
        );
    }

    public function test_cycle_sets_cycle_clause(): void
    {
        $cycleClause = cte('t', select(star())->from(table('users')))
            ->cycle(['id', 'link'], 'is_cycle', 'path', literal('Y'), literal('N'))
            ->toAst()
            ->getCommonTableExpr()
            ?->getCycleClause();

        static::assertNotNull($cycleClause);
        static::assertCount(2, $cycleClause->getCycleColList());
        static::assertSame('id', $cycleClause->getCycleColList()[0]->getString()?->getSval());
        static::assertSame('link', $cycleClause->getCycleColList()[1]->getString()?->getSval());
        static::assertSame('is_cycle', $cycleClause->getCycleMarkColumn());
        static::assertSame('path', $cycleClause->getCyclePathColumn());
        static::assertSame(
            literal('Y')->toAst()->serializeToString(),
            $cycleClause->getCycleMarkValue()?->serializeToString(),
        );
        static::assertSame(
            literal('N')->toAst()->serializeToString(),
            $cycleClause->getCycleMarkDefault()?->serializeToString(),
        );
    }

    public function test_cycle_without_mark_values_uses_true_and_false(): void
    {
        $cycleClause = cte('t', select(star())->from(table('users')))
            ->cycle(['id'], 'is_cycle', 'path')
            ->toAst()
            ->getCommonTableExpr()
            ?->getCycleClause();

        static::assertNotNull($cycleClause);
        static::assertSame(
            literal(true)->toAst()->serializeToString(),
            $cycleClause->getCycleMarkValue()?->serializeToString(),
        );
        static::assertSame(
            literal(false)->toAst()->serializeToString(),
            $cycleClause->getCycleMarkDefault()?->serializeToString(),
        );
    }

    public function test_materialization_and_columns_keep_search_and_cycle_clauses(): void
    {
        $cte = cte('t', select(star())->from(table('users')))
            ->searchDepthFirst(['id'], 'ord')
            ->cycle(['id'], 'is_cycle', 'path');

        foreach ([$cte->materialized(), $cte->notMaterialized(), $cte->withColumns(['id', 'link'])] as $copy) {
            $commonTableExpr = $copy->toAst()->getCommonTableExpr();

            static::assertSame('ord', $commonTableExpr?->getSearchClause()?->getSearchSeqColumn());
            static::assertSame('is_cycle', $commonTableExpr->getCycleClause()?->getCycleMarkColumn());
        }
    }

    public function test_search_breadth_first_sets_search_clause(): void
    {
        $searchClause = cte('t', select(star())->from(table('users')))
            ->searchBreadthFirst(['n', 'm'], 'ord')
            ->toAst()
            ->getCommonTableExpr()
            ?->getSearchClause();

        static::assertNotNull($searchClause);
        static::assertCount(2, $searchClause->getSearchColList());
        static::assertSame('n', $searchClause->getSearchColList()[0]->getString()?->getSval());
        static::assertSame('m', $searchClause->getSearchColList()[1]->getString()?->getSval());
        static::assertTrue($searchClause->getSearchBreadthFirst());
        static::assertSame('ord', $searchClause->getSearchSeqColumn());
    }

    public function test_search_depth_first_sets_search_clause(): void
    {
        $searchClause = cte('t', select(star())->from(table('users')))
            ->searchDepthFirst(['n'], 'ord')
            ->toAst()
            ->getCommonTableExpr()
            ?->getSearchClause();

        static::assertNotNull($searchClause);
        static::assertCount(1, $searchClause->getSearchColList());
        static::assertSame('n', $searchClause->getSearchColList()[0]->getString()?->getSval());
        static::assertFalse($searchClause->getSearchBreadthFirst());
        static::assertSame('ord', $searchClause->getSearchSeqColumn());
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_search_requires_at_least_one_column(bool $breadthFirst): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('SEARCH needs at least one column');

        $cte = cte('t', select(star())->from(table('users')));

        $breadthFirst ? $cte->searchBreadthFirst([], 'ord') : $cte->searchDepthFirst([], 'ord');
    }

    public function test_immutable_materialized_method(): void
    {
        $cte = cte('expensive', select(star())->from(table('users')));

        static::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());

        $materialized = $cte->materialized();

        static::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());
        static::assertSame(CTEMaterialization::MATERIALIZED, $materialized->materialization());
        static::assertNotSame($cte, $materialized);
    }

    public function test_immutable_not_materialized_method(): void
    {
        $cte = cte('temp', select(star())->from(table('users')));

        $notMaterialized = $cte->notMaterialized();

        static::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());
        static::assertSame(CTEMaterialization::NOT_MATERIALIZED, $notMaterialized->materialization());
        static::assertNotSame($cte, $notMaterialized);
    }

    public function test_immutable_with_columns_method(): void
    {
        $cte = cte('users', select(star())->from(table('users')));

        static::assertSame([], $cte->columnNames());

        $withColumns = $cte->withColumns(['id', 'name']);

        static::assertSame([], $cte->columnNames());
        static::assertSame(['id', 'name'], $withColumns->columnNames());
        static::assertNotSame($cte, $withColumns);
    }

    public function test_roundtrip_conversion_simple(): void
    {
        $original = cte('test', select(star())->from(table('users')));

        $node = $original->toAst();
        $reconstructed = CTE::fromAst($node);

        static::assertSame($original->name(), $reconstructed->name());
        static::assertSame($original->columnNames(), $reconstructed->columnNames());
        static::assertSame($original->materialization(), $reconstructed->materialization());
    }

    public function test_roundtrip_conversion_with_all_options(): void
    {
        $original = cte(
            'complex',
            select(star())->from(table('users')),
            ['a', 'b', 'c'],
            CTEMaterialization::MATERIALIZED,
        );

        $node = $original->toAst();
        $reconstructed = CTE::fromAst($node);

        static::assertSame($original->name(), $reconstructed->name());
        static::assertSame($original->columnNames(), $reconstructed->columnNames());
        static::assertSame($original->materialization(), $reconstructed->materialization());
    }
}
