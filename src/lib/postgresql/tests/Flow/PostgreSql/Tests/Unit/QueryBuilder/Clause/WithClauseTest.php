<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Clause\CTE;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use PHPUnit\Framework\TestCase;

use function count;

final class WithClauseTest extends TestCase
{
    public function test_converts_multiple_ctes_to_ast(): void
    {
        $query1 = $this->createMockSelectNode();
        $query2 = $this->createMockSelectNode();

        $cte1 = new CTE('active', $query1);
        $cte2 = new CTE('inactive', $query2);

        $withClause = new WithClause([$cte1, $cte2]);

        $node = $withClause->toAst();

        $protoWithClause = $node->getWithClause();
        static::assertNotNull($protoWithClause);

        $ctes = $protoWithClause->getCtes();
        static::assertNotNull($ctes);
        static::assertCount(2, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        static::assertNotNull($firstCte);
        static::assertSame('active', $firstCte->getCtename());

        $secondCte = $ctes[1]->getCommonTableExpr();
        static::assertNotNull($secondCte);
        static::assertSame('inactive', $secondCte->getCtename());
    }

    public function test_converts_single_cte_to_ast(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('users', $query);
        $withClause = new WithClause([$cte]);

        $node = $withClause->toAst();

        $protoWithClause = $node->getWithClause();
        static::assertNotNull($protoWithClause);

        $ctes = $protoWithClause->getCtes();
        static::assertNotNull($ctes);
        static::assertCount(1, $ctes);

        $firstCte = $ctes[0]->getCommonTableExpr();
        static::assertNotNull($firstCte);
        static::assertSame('users', $firstCte->getCtename());
    }

    public function test_getters(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('test', $query);
        $withClause = new WithClause([$cte], true);

        static::assertCount(1, $withClause->ctes());
        static::assertSame($cte, $withClause->ctes()[0]);
        static::assertTrue($withClause->recursive());
    }

    public function test_immutable_add_method(): void
    {
        $query1 = $this->createMockSelectNode();
        $query2 = $this->createMockSelectNode();

        $cte1 = new CTE('first', $query1);
        $cte2 = new CTE('second', $query2);

        $original = new WithClause([$cte1]);

        static::assertCount(1, $original->ctes());

        $modified = $original->add($cte2);

        static::assertCount(1, $original->ctes());
        static::assertCount(2, $modified->ctes());
        static::assertNotSame($original, $modified);
        static::assertSame($cte1, $modified->ctes()[0]);
        static::assertSame($cte2, $modified->ctes()[1]);
    }

    public function test_recursive_with_clause_to_ast(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('hierarchy', $query, recursive: true);
        $withClause = new WithClause([$cte], recursive: true);

        $node = $withClause->toAst();

        $protoWithClause = $node->getWithClause();
        static::assertNotNull($protoWithClause);
        static::assertTrue($protoWithClause->getRecursive());
    }

    public function test_roundtrip_conversion_multiple_ctes(): void
    {
        $query1 = $this->createMockSelectNode();
        $query2 = $this->createMockSelectNode();
        $query3 = $this->createMockSelectNode();

        $cte1 = new CTE('first', $query1, ['a', 'b']);
        $cte2 = new CTE('second', $query2);
        $cte3 = new CTE('third', $query3, ['x']);

        $original = new WithClause([$cte1, $cte2, $cte3], true);

        $node = $original->toAst();
        $protoWithClause = $node->getWithClause();
        static::assertNotNull($protoWithClause);
        $reconstructed = WithClause::fromAst($protoWithClause);

        static::assertCount(3, $reconstructed->ctes());
        static::assertTrue($reconstructed->recursive());

        static::assertSame('first', $reconstructed->ctes()[0]->name());
        static::assertSame(['a', 'b'], $reconstructed->ctes()[0]->columnNames());

        static::assertSame('second', $reconstructed->ctes()[1]->name());
        static::assertSame([], $reconstructed->ctes()[1]->columnNames());

        static::assertSame('third', $reconstructed->ctes()[2]->name());
        static::assertSame(['x'], $reconstructed->ctes()[2]->columnNames());
    }

    public function test_roundtrip_conversion_single_cte(): void
    {
        $query = $this->createMockSelectNode();
        $cte = new CTE('test', $query);
        $original = new WithClause([$cte]);

        $node = $original->toAst();
        $protoWithClause = $node->getWithClause();
        static::assertNotNull($protoWithClause);
        $reconstructed = WithClause::fromAst($protoWithClause);

        static::assertCount(count($original->ctes()), $reconstructed->ctes());
        static::assertSame($original->recursive(), $reconstructed->recursive());
        static::assertSame($original->ctes()[0]->name(), $reconstructed->ctes()[0]->name());
    }

    private function createMockSelectNode(): Node
    {
        $selectStmt = new SelectStmt();
        $node = new Node();
        $node->setSelectStmt($selectStmt);

        return $node;
    }
}
