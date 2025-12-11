<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Domain;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{CollateClause, ConstrType, Constraint, CreateDomainStmt, Node, PBString};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Schema\DataType;

final readonly class CreateDomainBuilder implements CreateDomainOptionsStep, CreateDomainTypeStep
{
    use AstToSql;

    /**
     * @param list<Constraint> $constraints
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private ?DataType $dataType = null,
        private ?string $collation = null,
        private array $constraints = [],
        private ?string $currentConstraintName = null,
    ) {
    }

    public static function create(string $name) : CreateDomainTypeStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function as(DataType $dataType) : CreateDomainOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $dataType,
            $this->collation,
            $this->constraints,
            $this->currentConstraintName,
        );
    }

    public function check(string $expression) : CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_CHECK);
        $constraint->setRawExpr($this->parseExpression($expression));

        if ($this->currentConstraintName !== null) {
            $constraint->setConname($this->currentConstraintName);
        }

        $newConstraints = $this->constraints;
        $newConstraints[] = $constraint;

        return new self(
            $this->name,
            $this->schema,
            $this->dataType,
            $this->collation,
            $newConstraints,
            null,
        );
    }

    public function collate(string $collation) : CreateDomainOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->dataType,
            $collation,
            $this->constraints,
            $this->currentConstraintName,
        );
    }

    public function constraint(string $name) : CreateDomainOptionsStep
    {
        return new self(
            $this->name,
            $this->schema,
            $this->dataType,
            $this->collation,
            $this->constraints,
            $name,
        );
    }

    public function default(string $expression) : CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_DEFAULT);
        $constraint->setRawExpr($this->parseExpression($expression));

        $newConstraints = $this->constraints;
        $newConstraints[] = $constraint;

        return new self(
            $this->name,
            $this->schema,
            $this->dataType,
            $this->collation,
            $newConstraints,
            $this->currentConstraintName,
        );
    }

    public function notNull() : CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_NOTNULL);

        if ($this->currentConstraintName !== null) {
            $constraint->setConname($this->currentConstraintName);
        }

        $newConstraints = $this->constraints;
        $newConstraints[] = $constraint;

        return new self(
            $this->name,
            $this->schema,
            $this->dataType,
            $this->collation,
            $newConstraints,
            null,
        );
    }

    public function null() : CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_NULL);

        $newConstraints = $this->constraints;
        $newConstraints[] = $constraint;

        return new self(
            $this->name,
            $this->schema,
            $this->dataType,
            $this->collation,
            $newConstraints,
            $this->currentConstraintName,
        );
    }

    public function toAst() : CreateDomainStmt
    {
        $stmt = new CreateDomainStmt();

        $domainNameNodes = [];

        if ($this->schema !== null) {
            $str = new PBString();
            $str->setSval($this->schema);
            $node = new Node();
            $node->setString($str);
            $domainNameNodes[] = $node;
        }

        $str = new PBString();
        $str->setSval($this->name);
        $node = new Node();
        $node->setString($str);
        $domainNameNodes[] = $node;

        $stmt->setDomainname($domainNameNodes);

        if ($this->dataType !== null) {
            $stmt->setTypeName($this->dataType->toAst());
        }

        if ($this->collation !== null) {
            $collClause = new CollateClause();

            $collNameNodes = [];
            $str = new PBString();
            $str->setSval($this->collation);
            $node = new Node();
            $node->setString($str);
            $collNameNodes[] = $node;

            $collClause->setCollname($collNameNodes);
            $stmt->setCollClause($collClause);
        }

        if ($this->constraints !== []) {
            $constraintNodes = [];

            foreach ($this->constraints as $constraint) {
                $node = new Node();
                $node->setConstraint($constraint);
                $constraintNodes[] = $node;
            }

            $stmt->setConstraints($constraintNodes);
        }

        return $stmt;
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
