<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\Protobuf\AST\CollateClause;
use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\Protobuf\AST\CreateDomainStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

final readonly class CreateDomainBuilder implements CreateDomainOptionsStep, CreateDomainTypeStep
{
    use AstToSql;

    /**
     * @param list<Constraint> $constraints
     */
    private function __construct(
        private string $name,
        private ?string $schema = null,
        private ?ColumnType $dataType = null,
        private ?string $collation = null,
        private array $constraints = [],
        private ?string $currentConstraintName = null,
    ) {}

    public static function create(string $name): CreateDomainTypeStep
    {
        $identifier = QualifiedIdentifier::parse($name);

        return new self($identifier->name(), $identifier->schema());
    }

    public function as(ColumnType $dataType): CreateDomainOptionsStep
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

    public function check(Condition $condition): CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_CHECK);
        $constraint->setIsEnforced(true);
        $constraint->setRawExpr($condition->toAst());

        if ($this->currentConstraintName !== null) {
            $constraint->setConname($this->currentConstraintName);
        }

        $newConstraints = $this->constraints;
        $newConstraints[] = $constraint;

        return new self($this->name, $this->schema, $this->dataType, $this->collation, $newConstraints, null);
    }

    public function collate(string $collation): CreateDomainOptionsStep
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

    public function constraint(string $name): CreateDomainOptionsStep
    {
        return new self($this->name, $this->schema, $this->dataType, $this->collation, $this->constraints, $name);
    }

    public function default(Expression $expression): CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_DEFAULT);
        $constraint->setRawExpr($expression->toAst());

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

    public function notNull(): CreateDomainOptionsStep
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_NOTNULL);

        if ($this->currentConstraintName !== null) {
            $constraint->setConname($this->currentConstraintName);
        }

        $newConstraints = $this->constraints;
        $newConstraints[] = $constraint;

        return new self($this->name, $this->schema, $this->dataType, $this->collation, $newConstraints, null);
    }

    public function null(): CreateDomainOptionsStep
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

    public function toAst(): CreateDomainStmt
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
}
