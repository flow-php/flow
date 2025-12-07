<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Constraint;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{ConstrType, Constraint, Node};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

final readonly class CheckConstraint implements TableConstraint
{
    private function __construct(
        private string $expression,
        private ?string $name = null,
        private bool $noInherit = false,
    ) {
    }

    public static function create(string $expression) : self
    {
        return new self($expression);
    }

    public function name(string $name) : self
    {
        return new self($this->expression, $name, $this->noInherit);
    }

    public function noInherit() : self
    {
        return new self($this->expression, $this->name, true);
    }

    public function toAst() : Constraint
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_CHECK);

        if ($this->name !== null) {
            $constraint->setConname($this->name);
        }

        if ($this->noInherit) {
            $constraint->setIsNoInherit(true);
        }

        $constraint->setRawExpr($this->parseExpression($this->expression));

        return $constraint;
    }

    private function parseExpression(string $expression) : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT {$expression} AS x");

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
