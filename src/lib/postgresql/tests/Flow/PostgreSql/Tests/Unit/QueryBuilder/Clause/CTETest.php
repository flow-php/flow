<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Clause\CTE;
use Flow\PostgreSql\QueryBuilder\Clause\CTEMaterialization;
use PHPUnit\Framework\TestCase;

final class CTETest extends TestCase
{
    public function test_converts_simple_cte_to_ast(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('active_users', $query);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        static::assertNotNull($commonTableExpr);
        static::assertSame('active_users', $commonTableExpr->getCtename());
        static::assertSame($query, $commonTableExpr->getCtequery());
        static::assertFalse($commonTableExpr->getCterecursive());
    }

    public function test_converts_to_ast_with_column_names(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('totals', $query, ['category', 'amount']);

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
        $query = $this->createMockSelectNode();
        $cte = new CTE('test_cte', $query, ['col1', 'col2'], CTEMaterialization::MATERIALIZED, true);

        static::assertSame('test_cte', $cte->name());
        static::assertSame($query, $cte->query());
        static::assertSame(['col1', 'col2'], $cte->columnNames());
        static::assertSame(CTEMaterialization::MATERIALIZED, $cte->materialization());
        static::assertTrue($cte->recursive());
    }

    public function test_immutable_materialized_method(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('expensive', $query);

        static::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());

        $materialized = $cte->materialized();

        static::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());
        static::assertSame(CTEMaterialization::MATERIALIZED, $materialized->materialization());
        static::assertNotSame($cte, $materialized);
    }

    public function test_immutable_not_materialized_method(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('temp', $query);

        $notMaterialized = $cte->notMaterialized();

        static::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());
        static::assertSame(CTEMaterialization::NOT_MATERIALIZED, $notMaterialized->materialization());
        static::assertNotSame($cte, $notMaterialized);
    }

    public function test_immutable_with_columns_method(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('users', $query);

        static::assertSame([], $cte->columnNames());

        $withColumns = $cte->withColumns(['id', 'name']);

        static::assertSame([], $cte->columnNames());
        static::assertSame(['id', 'name'], $withColumns->columnNames());
        static::assertNotSame($cte, $withColumns);
    }

    public function test_recursive_cte_to_ast(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('hierarchy', $query, recursive: true);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        static::assertNotNull($commonTableExpr);
        static::assertTrue($commonTableExpr->getCterecursive());
    }

    public function test_roundtrip_conversion_simple(): void
    {
        $query = $this->createMockSelectNode();
        $original = new CTE('test', $query);

        $node = $original->toAst();
        $reconstructed = CTE::fromAst($node);

        static::assertSame($original->name(), $reconstructed->name());
        static::assertSame($original->columnNames(), $reconstructed->columnNames());
        static::assertSame($original->materialization(), $reconstructed->materialization());
        static::assertSame($original->recursive(), $reconstructed->recursive());
    }

    public function test_roundtrip_conversion_with_all_options(): void
    {
        $query = $this->createMockSelectNode();
        $original = new CTE('complex', $query, ['a', 'b', 'c'], CTEMaterialization::MATERIALIZED, true);

        $node = $original->toAst();
        $reconstructed = CTE::fromAst($node);

        static::assertSame($original->name(), $reconstructed->name());
        static::assertSame($original->columnNames(), $reconstructed->columnNames());
        static::assertSame($original->materialization(), $reconstructed->materialization());
        static::assertSame($original->recursive(), $reconstructed->recursive());
    }

    private function createMockSelectNode(): Node
    {
        $selectStmt = new SelectStmt();
        $node = new Node();
        $node->setSelectStmt($selectStmt);

        return $node;
    }
}
