<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint, Node, PBString};

final readonly class UniqueConstraint implements TableConstraint
{
    /**
     * @param list<string> $columns
     */
    private function __construct(
        private array $columns,
        private ?string $name = null,
        private bool $nullsNotDistinct = false,
    ) {
    }

    public static function create(string ...$columns) : self
    {
        return new self(\array_values($columns));
    }

    public function name(string $name) : self
    {
        return new self($this->columns, $name, $this->nullsNotDistinct);
    }

    public function nullsNotDistinct() : self
    {
        return new self($this->columns, $this->name, true);
    }

    public function toAst() : Constraint
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_UNIQUE);

        if ($this->name !== null) {
            $constraint->setConname($this->name);
        }

        if ($this->nullsNotDistinct) {
            $constraint->setNullsNotDistinct(true);
        }

        $keys = [];

        foreach ($this->columns as $column) {
            $keys[] = $this->createStringNode($column);
        }

        $constraint->setKeys($keys);

        return $constraint;
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
