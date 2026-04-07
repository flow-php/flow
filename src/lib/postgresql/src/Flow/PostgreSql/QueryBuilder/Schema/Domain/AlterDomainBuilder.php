<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\Protobuf\AST\{AlterDomainStmt, ConstrType, Constraint, DropBehavior, Node, PBString};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

final readonly class AlterDomainBuilder implements AlterDomainActionStep, AlterDomainFinalStep
{
    use AstToSql;

    private function __construct(
        private string $name,
        private ?string $schema = null,
        private string $subtype = '',
        private ?string $constraintName = null,
        private ?Expression $expression = null,
        private int $behavior = DropBehavior::DROP_RESTRICT,
        private bool $missingOk = false,
    ) {
    }

    public static function create(string $name) : AlterDomainActionStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function addConstraint(string $name, Condition $condition) : AlterDomainFinalStep
    {
        return new self(
            $this->name,
            $this->schema,
            'C',
            $name,
            $condition,
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

    public function setDefault(Expression $expression) : AlterDomainFinalStep
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
            $constraint->setRawExpr($this->expression->toAst());

            if ($this->constraintName !== null) {
                $constraint->setConname($this->constraintName);
            }

            $defNode = new Node();
            $defNode->setConstraint($constraint);
            $stmt->setDef($defNode);
        } elseif ($this->subtype === 'T' && $this->expression !== null) {
            $stmt->setDef($this->expression->toAst());
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
}
