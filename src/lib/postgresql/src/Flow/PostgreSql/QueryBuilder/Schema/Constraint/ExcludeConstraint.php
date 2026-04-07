<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint, Node, PBString};
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

final readonly class ExcludeConstraint implements TableConstraint
{
    /**
     * @param list<array{element: Expression, operator: string}> $elements
     */
    private function __construct(
        private string $accessMethod,
        private array $elements = [],
        private ?string $name = null,
        private ?Condition $whereCondition = null,
    ) {
    }

    public static function create(string $accessMethod = 'gist') : self
    {
        return new self($accessMethod);
    }

    public function element(Expression $element, string $operator) : self
    {
        return new self(
            $this->accessMethod,
            [...$this->elements, ['element' => $element, 'operator' => $operator]],
            $this->name,
            $this->whereCondition,
        );
    }

    public function name(string $name) : self
    {
        return new self(
            $this->accessMethod,
            $this->elements,
            $name,
            $this->whereCondition,
        );
    }

    public function toAst() : Constraint
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_EXCLUSION);
        $constraint->setAccessMethod($this->accessMethod);

        if ($this->name !== null) {
            $constraint->setConname($this->name);
        }

        if ($this->elements !== []) {
            $exclusions = [];

            foreach ($this->elements as $element) {
                $exclusions[] = $element['element']->toAst();
                $exclusions[] = $this->createOperatorNode($element['operator']);
            }

            $constraint->setExclusions($exclusions);
        }

        if ($this->whereCondition !== null) {
            $constraint->setWhereClause($this->whereCondition->toAst());
        }

        return $constraint;
    }

    public function where(Condition $condition) : self
    {
        return new self(
            $this->accessMethod,
            $this->elements,
            $this->name,
            $condition,
        );
    }

    private function createOperatorNode(string $operator) : Node
    {
        $operatorList = new PBList();
        $operatorItems = [];

        $str = new PBString();
        $str->setSval($operator);

        $node = new Node();
        $node->setString($str);
        $operatorItems[] = $node;

        $operatorList->setItems($operatorItems);

        $resultNode = new Node();
        $resultNode->setList($operatorList);

        return $resultNode;
    }
}
