<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint};
use Flow\PostgreSql\QueryBuilder\Condition\Condition;

final readonly class CheckConstraint implements TableConstraint
{
    private function __construct(
        private Condition $condition,
        private ?string $name = null,
        private bool $noInherit = false,
    ) {
    }

    public static function create(Condition $condition) : self
    {
        return new self($condition);
    }

    public function name(string $name) : self
    {
        return new self($this->condition, $name, $this->noInherit);
    }

    public function noInherit() : self
    {
        return new self($this->condition, $this->name, true);
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

        $constraint->setRawExpr($this->condition->toAst());

        return $constraint;
    }
}
