<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\AST\Transformers\TypeCastStripper;
use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ExpressionParser
{
    public function __construct(
        private Parser $parser,
        private TypeCastStripper $stripper = new TypeCastStripper(),
    ) {
    }

    /**
     * Normalize an expression by stripping implicit type casts that PostgreSQL adds.
     *
     * PostgreSQL's pg_get_expr() returns expressions with explicit casts like
     * `to_tsvector('english'::regconfig, name::text)` even though the original expression
     * was `to_tsvector('english', name)`. This method parses the expression, strips all
     * TypeCast nodes from the AST, and deparses back to produce a canonical form
     * that matches what a user would write in a generation expression, default, check
     * constraint, index predicate, etc.
     */
    public function normalize(string $expression) : string
    {
        $parsed = $this->parser->parse("SELECT {$expression} AS x");
        $parsed->traverse($this->stripper);

        return \substr($parsed->deparse(), 7, -5);
    }

    public function parse(string $expression) : Node
    {
        $parsed = $this->parser->parse("SELECT {$expression} AS x");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $selectStmt = $stmts[0]->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $resTarget = $targetList[0]->getResTarget();

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
