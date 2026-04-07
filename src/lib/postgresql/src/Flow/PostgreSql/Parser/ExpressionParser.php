<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ExpressionParser
{
    public function __construct(
        private Parser $parser,
    ) {
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
