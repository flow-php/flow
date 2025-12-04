<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Clause;

use Flow\PgQuery\Protobuf\AST\{Node, SelectStmt};
use Flow\PgQuery\QueryBuilder\Clause\{CTE, WithClause};
use PHPUnit\Framework\TestCase;

final class WithClauseTest extends TestCase
{
    public function test_converts_multiple_ctes_to_ast() : void
    {
        $query1 = $this->createMockSelectNode();
        $query2 = $this->createMockSelectNode();

        $cte1 = new CTE('active', $query1);
        $cte2 = new CTE('inactive', $query2);

        $withClause = new WithClause([$cte1, $cte2]);

        $node = $withClause->toAst();

        $protoWithClause = $node->getWithClause();
        self::assertNotNull($protoWithClause);

        $ctes = $protoWithClause->getCtes();
        self::assertNotNull($ctes);
        self::assertCount(2, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        self::assertNotNull($firstCte);
        self::assertSame('active', $firstCte->getCtename());

        $secondCte = $ctes[1]->getCommonTableExpr();
        self::assertNotNull($secondCte);
        self::assertSame('inactive', $secondCte->getCtename());
    }

    public function test_converts_single_cte_to_ast() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('users', $query);
        $withClause = new WithClause([$cte]);

        $node = $withClause->toAst();

        $protoWithClause = $node->getWithClause();
        self::assertNotNull($protoWithClause);

        $ctes = $protoWithClause->getCtes();
        self::assertNotNull($ctes);
        self::assertCount(1, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        self::assertNotNull($firstCte);
        self::assertSame('users', $firstCte->getCtename());
    }

    public function test_getters() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('test', $query);
        $withClause = new WithClause([$cte], true);

        self::assertCount(1, $withClause->ctes());
        self::assertSame($cte, $withClause->ctes()[0]);
        self::assertTrue($withClause->recursive());
    }

    public function test_immutable_add_method() : void
    {
        $query1 = $this->createMockSelectNode();
        $query2 = $this->createMockSelectNode();

        $cte1 = new CTE('first', $query1);
        $cte2 = new CTE('second', $query2);

        $original = new WithClause([$cte1]);

        self::assertCount(1, $original->ctes());

        $modified = $original->add($cte2);

        self::assertCount(1, $original->ctes());
        self::assertCount(2, $modified->ctes());
        self::assertNotSame($original, $modified);
        self::assertSame($cte1, $modified->ctes()[0]);
        self::assertSame($cte2, $modified->ctes()[1]);
    }

    public function test_recursive_with_clause_to_ast() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('hierarchy', $query, recursive: true);
        $withClause = new WithClause([$cte], recursive: true);

        $node = $withClause->toAst();

        $protoWithClause = $node->getWithClause();
        self::assertNotNull($protoWithClause);
        self::assertTrue($protoWithClause->getRecursive());
    }

    public function test_roundtrip_conversion_multiple_ctes() : void
    {
        $query1 = $this->createMockSelectNode();
        $query2 = $this->createMockSelectNode();
        $query3 = $this->createMockSelectNode();

        $cte1 = new CTE('first', $query1, ['a', 'b']);
        $cte2 = new CTE('second', $query2);
        $cte3 = new CTE('third', $query3, ['x']);

        $original = new WithClause([$cte1, $cte2, $cte3], true);

        $node = $original->toAst();
        $reconstructed = WithClause::fromAst($node);

        self::assertCount(3, $reconstructed->ctes());
        self::assertTrue($reconstructed->recursive());

        self::assertSame('first', $reconstructed->ctes()[0]->name());
        self::assertSame(['a', 'b'], $reconstructed->ctes()[0]->columnNames());

        self::assertSame('second', $reconstructed->ctes()[1]->name());
        self::assertSame([], $reconstructed->ctes()[1]->columnNames());

        self::assertSame('third', $reconstructed->ctes()[2]->name());
        self::assertSame(['x'], $reconstructed->ctes()[2]->columnNames());
    }

    public function test_roundtrip_conversion_single_cte() : void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('test', $query);
        $original = new WithClause([$cte]);

        $node = $original->toAst();
        $reconstructed = WithClause::fromAst($node);

        self::assertCount(\count($original->ctes()), $reconstructed->ctes());
        self::assertSame($original->recursive(), $reconstructed->recursive());
        self::assertSame($original->ctes()[0]->name(), $reconstructed->ctes()[0]->name());
    }

    private function createMockSelectNode() : Node
    {
        $selectStmt = new SelectStmt();
        $node = new Node();
        $node->setSelectStmt($selectStmt);

        return $node;
    }
}
