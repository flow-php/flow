<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;
use Flow\PostgreSql\AST\Nodes\Statement\DeleteStatement;
use Flow\PostgreSql\AST\Nodes\Statement\InsertStatement;
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\Protobuf\AST\DeleteStmt;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use PHPUnit\Framework\TestCase;

final class StatementAssertTest extends TestCase
{
    public function test_assert_returns_statement_when_type_matches(): void
    {
        $selectStmt = new SelectStmt();
        $statement = new SelectStatement($selectStmt);

        $result = $statement->assert(SelectStatement::class);

        static::assertSame($statement, $result);
    }

    public function test_assert_throws_when_type_does_not_match(): void
    {
        $selectStmt = new SelectStmt();
        $statement = new SelectStatement($selectStmt);

        $this->expectException(InvalidStatementException::class);
        $this->expectExceptionMessage(
            'Expected statement of type Flow\PostgreSql\AST\Nodes\Statement\InsertStatement, got Flow\PostgreSql\AST\Nodes\Statement\SelectStatement',
        );

        $statement->assert(InsertStatement::class);
    }

    public function test_assert_works_with_different_statement_types(): void
    {
        $deleteStmt = new DeleteStmt();
        $statement = new DeleteStatement($deleteStmt);

        $result = $statement->assert(DeleteStatement::class);

        static::assertSame($statement, $result);
    }
}
