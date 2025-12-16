<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, UnsupportedNodeException};

/**
 * Raw SQL expression (escape hatch for unsupported expressions).
 *
 * Use this when you need to include raw SQL that isn't supported by the query builder.
 * The SQL will be parsed to ensure it's valid PostgreSQL syntax.
 *
 * SECURITY WARNING: This class accepts raw SQL without parameterization.
 * SQL injection is possible if used with untrusted user input.
 * Only use with trusted, validated input. For user-provided values,
 * use parameterized queries with Parameter expressions instead.
 *
 * @see Parameter For safe parameterized values
 */
final readonly class RawExpression implements Expression
{
    public function __construct(
        private string $sql,
    ) {
        if ($this->sql === '') {
            throw new \InvalidArgumentException('RawExpression SQL cannot be empty');
        }
    }

    public static function fromAst(Node $node) : static
    {
        throw UnsupportedNodeException::cannotReconstruct('RawExpression - cannot convert AST back to raw SQL string');
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function sql() : string
    {
        return $this->sql;
    }

    public function toAst() : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT {$this->sql} AS x");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $firstStmt = $stmts[0];
        $selectStmt = $firstStmt->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $firstTarget = $targetList[0];
        $resTarget = $firstTarget->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $val = $resTarget->getVal();

        if ($val === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        return $val;
    }
}
