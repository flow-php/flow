<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\{A_Const, Boolean as PBBoolean, ColumnDef, ConstrType, Constraint, Integer as PBInteger, Node, PBFloat, PBString, RangeVar};
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\Schema\IdentityGeneration;

final readonly class ColumnDefinition
{
    /**
     * @param list<Constraint> $constraints
     */
    private function __construct(
        private string $name,
        private ColumnType $type,
        private bool $notNull = false,
        private ?Node $defaultValue = null,
        private ?IdentityGeneration $identity = null,
        private ?Node $generatedExpression = null,
        private array $constraints = [],
    ) {
    }

    public static function create(string $name, ColumnType $type) : self
    {
        return new self($name, $type);
    }

    public function check(Condition $condition) : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_CHECK);
        $constraint->setRawExpr($condition->toAst());

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    public function default(bool|float|int|string|Expression|null $value) : self
    {
        $node = $value instanceof Expression
            ? $value->toAst()
            : $this->createLiteralNode($value);

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $node,
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function defaultRaw(Expression $expression) : self
    {
        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $expression->toAst(),
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function generatedAs(Expression $expression) : self
    {
        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $expression->toAst(),
            $this->constraints,
        );
    }

    public function identity(IdentityGeneration $type = IdentityGeneration::ALWAYS) : self
    {
        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $type,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function notNull() : self
    {
        return new self(
            $this->name,
            $this->type,
            true,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function nullable() : self
    {
        return new self(
            $this->name,
            $this->type,
            false,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function primaryKey() : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_PRIMARY);

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    public function references(string $table, ?string $column = null, ?string $schema = null) : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_FOREIGN);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($table);

        if ($schema !== null) {
            $rangeVar->setSchemaname($schema);
        }

        $constraint->setPktable($rangeVar);

        if ($column !== null) {
            $constraint->setPkAttrs([$this->createStringNode($column)]);
        }

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    public function toAst() : ColumnDef
    {
        $columnDef = new ColumnDef();
        $columnDef->setColname($this->name);
        $columnDef->setTypeName($this->type->toAst());
        $columnDef->setIsLocal(true);

        $allConstraints = [];

        if ($this->notNull) {
            $notNullConstraint = new Constraint();
            $notNullConstraint->setContype(ConstrType::CONSTR_NOTNULL);
            $allConstraints[] = $notNullConstraint;
        }

        if ($this->defaultValue !== null) {
            $defaultConstraint = new Constraint();
            $defaultConstraint->setContype(ConstrType::CONSTR_DEFAULT);
            $defaultConstraint->setRawExpr($this->defaultValue);
            $allConstraints[] = $defaultConstraint;
        }

        foreach ($this->constraints as $constraint) {
            $allConstraints[] = $constraint;
        }

        if ($this->identity !== null) {
            $identityConstraint = new Constraint();
            $identityConstraint->setContype(ConstrType::CONSTR_IDENTITY);
            $identityConstraint->setGeneratedWhen($this->identity->value);
            $allConstraints[] = $identityConstraint;
        }

        if ($this->generatedExpression !== null) {
            $columnDef->setGenerated('s');

            $generatedConstraint = new Constraint();
            $generatedConstraint->setContype(ConstrType::CONSTR_GENERATED);
            $generatedConstraint->setGeneratedWhen('a');
            $generatedConstraint->setRawExpr($this->generatedExpression);

            $allConstraints[] = $generatedConstraint;
        }

        if ($allConstraints !== []) {
            $constraintNodes = [];

            foreach ($allConstraints as $constraint) {
                $node = new Node();
                $node->setConstraint($constraint);
                $constraintNodes[] = $node;
            }

            $columnDef->setConstraints($constraintNodes);
        }

        return $columnDef;
    }

    public function unique() : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_UNIQUE);

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    private function createLiteralNode(bool|float|int|string|null $value) : Node
    {
        $aConst = new A_Const();

        if ($value === null) {
            $aConst->setIsnull(true);
        } elseif (\is_bool($value)) {
            /** @phpstan-ignore-next-line */
            $aConst->setBoolval((new PBBoolean())->setBoolval($value));
        } elseif (\is_int($value)) {
            /** @phpstan-ignore-next-line */
            $aConst->setIval((new PBInteger())->setIval($value));
        } elseif (\is_float($value)) {
            $aConst->setFval((new PBFloat())->setFval((string) $value));
        } else {
            $aConst->setSval((new PBString())->setSval($value));
        }

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function createStringNode(string $value) : Node
    {
        $str = new PBString();
        $str->setSval($value);

        $node = new Node();
        $node->setString($str);

        return $node;
    }
}
