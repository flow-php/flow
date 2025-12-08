<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Domain;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{AlterDomainStmt, ConstrType, Constraint, DropBehavior, Node, PBString};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

final readonly class AlterDomainBuilder implements AlterDomainActionStep, AlterDomainFinalStep
{
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private string $subtype = '',
        private ?string $constraintName = null,
        private ?string $expression = null,
        private int $behavior = DropBehavior::DROP_RESTRICT,
        private bool $missingOk = false,
    ) {
    }

    public static function create(string $name) : AlterDomainActionStep
    {
        $parts = \explode('.', $name);

        if (\count($parts) === 2) {
            return new self($parts[1], $parts[0]);
        }

        return new self($name);
    }

    public function addConstraint(string $name, string $expression) : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'C',
            $name,
            $expression,
            $this->behavior,
            $this->missingOk,
        );
    }

    public function cascade() : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->subtype,
            $this->constraintName,
            $this->expression,
            DropBehavior::DROP_CASCADE,
            $this->missingOk,
        );
    }

    public function dropConstraint(string $name) : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'X',
            $name,
            null,
            $this->behavior,
            $this->missingOk,
        );
    }

    public function dropDefault() : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'T',
            null,
            null,
            $this->behavior,
            $this->missingOk,
        );
    }

    public function dropNotNull() : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'N',
            null,
            null,
            $this->behavior,
            $this->missingOk,
        );
    }

    public function ifExists() : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->subtype,
            $this->constraintName,
            $this->expression,
            $this->behavior,
            true,
        );
    }

    public function restrict() : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->subtype,
            $this->constraintName,
            $this->expression,
            DropBehavior::DROP_RESTRICT,
            $this->missingOk,
        );
    }

    public function setDefault(string $expression) : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'T',
            null,
            $expression,
            $this->behavior,
            $this->missingOk,
        );
    }

    public function setNotNull() : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'O',
            null,
            null,
            $this->behavior,
            $this->missingOk,
        );
    }

    public function toAst() : AlterDomainStmt
    {
        $stmt = new AlterDomainStmt();
        $stmt->setSubtype($this->subtype);

        $typeNameNodes = [];

        if ($this->schema !== null) {
            $str = new PBString();
            $str->setSval($this->schema);
            $node = new Node();
            $node->setString($str);
            $typeNameNodes[] = $node;
        }

        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $typeNameNodes[] = $node;

        $stmt->setTypeName($typeNameNodes);

        if ($this->constraintName !== null) {
            $stmt->setName($this->constraintName);
        }

        if ($this->subtype === 'C' && $this->expression !== null) {
            $constraint = new Constraint();
            $constraint->setContype(ConstrType::CONSTR_CHECK);
            $constraint->setRawExpr($this->parseExpression($this->expression));

            if ($this->constraintName !== null) {
                $constraint->setConname($this->constraintName);
            }

            $defNode = new Node();
            $defNode->setConstraint($constraint);
            $stmt->setDef($defNode);
        } elseif ($this->subtype === 'T' && $this->expression !== null) {
            $stmt->setDef($this->parseExpression($this->expression));
        }

        $stmt->setBehavior($this->behavior);
        $stmt->setMissingOk($this->missingOk);

        return $stmt;
    }

    public function validateConstraint(string $name) : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'V',
            $name,
            null,
            $this->behavior,
            $this->missingOk,
        );
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
