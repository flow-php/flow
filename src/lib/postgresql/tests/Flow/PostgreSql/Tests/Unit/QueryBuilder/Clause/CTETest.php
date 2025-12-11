<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{Node, SelectStmt};
use Flow\PostgreSql\QueryBuilder\Clause\{CTE, CTEMaterialization};
use PHPUnit\Framework\TestCase;

final class CTETest extends TestCase
{
    public function test_converts_simple_cte_to_ast() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('active_users', $query);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        self::assertNotNull($commonTableExpr);
        self::assertSame('active_users', $commonTableExpr->getCtename());
        self::assertSame($query, $commonTableExpr->getCtequery());
        self::assertFalse($commonTableExpr->getCterecursive());
    }

    public function test_converts_to_ast_with_column_names() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('totals', $query, ['category', 'amount']);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        self::assertNotNull($commonTableExpr);

        $aliasColumns = $commonTableExpr->getAliascolnames();
        self::assertNotNull($aliasColumns);
        self::assertCount(2, $aliasColumns);

        $firstColumn = $aliasColumns[0]->getString();
        self::assertNotNull($firstColumn);
        self::assertSame('category', $firstColumn->getSval());

        $secondColumn = $aliasColumns[1]->getString();
        self::assertNotNull($secondColumn);
        self::assertSame('amount', $secondColumn->getSval());
    }

    public function test_cte_getters() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('test_cte', $query, ['col1', 'col2'], CTEMaterialization::MATERIALIZED, true);

        self::assertSame('test_cte', $cte->name());
        self::assertSame($query, $cte->query());
        self::assertSame(['col1', 'col2'], $cte->columnNames());
        self::assertSame(CTEMaterialization::MATERIALIZED, $cte->materialization());
        self::assertTrue($cte->recursive());
    }

    public function test_immutable_materialized_method() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('expensive', $query);

        self::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());

        $materialized = $cte->materialized();

        self::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());
        self::assertSame(CTEMaterialization::MATERIALIZED, $materialized->materialization());
        self::assertNotSame($cte, $materialized);
    }

    public function test_immutable_not_materialized_method() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('temp', $query);

        $notMaterialized = $cte->notMaterialized();

        self::assertSame(CTEMaterialization::DEFAULT, $cte->materialization());
        self::assertSame(CTEMaterialization::NOT_MATERIALIZED, $notMaterialized->materialization());
        self::assertNotSame($cte, $notMaterialized);
    }

    public function test_immutable_with_columns_method() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('users', $query);

        self::assertSame([], $cte->columnNames());

        $withColumns = $cte->withColumns(['id', 'name']);

        self::assertSame([], $cte->columnNames());
        self::assertSame(['id', 'name'], $withColumns->columnNames());
        self::assertNotSame($cte, $withColumns);
    }

    public function test_recursive_cte_to_ast() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('hierarchy', $query, recursive: true);

        $node = $cte->toAst();

        $commonTableExpr = $node->getCommonTableExpr();
        self::assertNotNull($commonTableExpr);
        self::assertTrue($commonTableExpr->getCterecursive());
    }

    public function test_roundtrip_conversion_simple() : void
    {
        $query = $this->createMockSelectNode();
        $original = new CTE('test', $query);

        $node = $original->toAst();
        $reconstructed = CTE::fromAst($node);

        self::assertSame($original->name(), $reconstructed->name());
        self::assertSame($original->columnNames(), $reconstructed->columnNames());
        self::assertSame($original->materialization(), $reconstructed->materialization());
        self::assertSame($original->recursive(), $reconstructed->recursive());
    }

    public function test_roundtrip_conversion_with_all_options() : void
    {
        $query = $this->createMockSelectNode();
        $original = new CTE('complex', $query, ['a', 'b', 'c'], CTEMaterialization::MATERIALIZED, true);

        $node = $original->toAst();
        $reconstructed = CTE::fromAst($node);

        self::assertSame($original->name(), $reconstructed->name());
        self::assertSame($original->columnNames(), $reconstructed->columnNames());
        self::assertSame($original->materialization(), $reconstructed->materialization());
        self::assertSame($original->recursive(), $reconstructed->recursive());
    }

    private function createMockSelectNode() : Node
    {
        $selectStmt = new SelectStmt();
        $node = new Node();
        $node->setSelectStmt($selectStmt);

        return $node;
    }
}
