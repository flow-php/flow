<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{IndexElem, InferClause, Node};
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Condition\{Condition, ConditionFactory};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a conflict target specification for ON CONFLICT clause.
 */
final readonly class ConflictTarget implements AstConvertible
{
    /**
     * @param list<string> $columns
     */
    public function __construct(
        private array $columns = [],
        private ?string $constraint = null,
        private ?Condition $whereClause = null,
    ) {
    }

    /**
     * @param list<string> $columns
     */
    public static function columns(array $columns) : self
    {
        return new self($columns);
    }

    public static function constraint(string $name) : self
    {
        return new self([], $name);
    }

    public static function fromAst(Node $node) : static
    {
        $inferClause = $node->getInferClause();

        if ($inferClause === null) {
            throw InvalidAstException::unexpectedNodeType('InferClause', 'unknown');
        }

        $constraint = $inferClause->getConname();

        if ($constraint === '') {
            $constraint = null;
        }

        $columns = [];
        $indexElems = $inferClause->getIndexElems();

        if ($indexElems !== null) {
            foreach ($indexElems as $indexElem) {
                $indexElemNode = $indexElem->getIndexElem();

                if ($indexElemNode !== null) {
                    $name = $indexElemNode->getName();

                    if ($name !== '') {
                        $columns[] = $name;
                    }
                }
            }
        }

        $whereClause = null;
        $whereNode = $inferClause->getWhereClause();

        if ($whereNode !== null) {
            $whereClause = ConditionFactory::fromAst($whereNode);
        }

        return new self($columns, $constraint, $whereClause);
    }

    /**
     * @return list<string>
     */
    public function getColumns() : array
    {
        return $this->columns;
    }

    public function getConstraint() : ?string
    {
        return $this->constraint;
    }

    public function toAst() : Node
    {
        $inferClause = new InferClause();

        if ($this->constraint !== null) {
            $inferClause->setConname($this->constraint);
        }

        if ($this->columns !== []) {
            $indexElems = [];

            foreach ($this->columns as $column) {
                $indexElem = new IndexElem();
                $indexElem->setName($column);

                $node = new Node();
                $node->setIndexElem($indexElem);

                $indexElems[] = $node;
            }

            $inferClause->setIndexElems($indexElems);
        }

        if ($this->whereClause !== null) {
            $inferClause->setWhereClause($this->whereClause->toAst());
        }

        $node = new Node();
        $node->setInferClause($inferClause);

        return $node;
    }

    public function where(Condition $condition) : self
    {
        return new self($this->columns, $this->constraint, $condition);
    }

    public function whereClause() : ?Condition
    {
        return $this->whereClause;
    }
}
