<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class TriggerDefinitionParser
{
    public function __construct(
        private ExpressionParser $expressionParser,
    ) {
    }

    public function parseWhenClause(string $triggerDef) : ?string
    {
        $parsed = $this->expressionParser->parseStatement($triggerDef);
        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $createTrigStmt = $stmts[0]->getStmt()?->getCreateTrigStmt();

        if ($createTrigStmt === null) {
            throw InvalidAstException::unexpectedNodeType('CreateTrigStmt', 'unknown');
        }

        $whenClause = $createTrigStmt->getWhenClause();

        if ($whenClause === null) {
            return null;
        }

        return $this->expressionParser->normalizeNode($whenClause);
    }
}
