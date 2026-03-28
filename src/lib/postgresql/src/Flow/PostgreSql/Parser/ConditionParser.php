<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ConditionParser
{
    public function __construct(
        private Parser $parser,
    ) {
    }

    public function parse(string $condition) : Node
    {
        $parsed = $this->parser->parse("SELECT * FROM t WHERE {$condition}");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $selectStmt = $stmts[0]->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $whereClause = $selectStmt->getWhereClause();

        if ($whereClause === null) {
            throw InvalidAstException::missingRequiredField('whereClause', 'SelectStmt');
        }

        return $whereClause;
    }
}
