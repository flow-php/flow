<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint, IndexElem, Node, PBString};
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Expression};

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
        private bool $deferrable = false,
        private bool $initiallyDeferred = false,
    ) {
    }

    public static function create(string $accessMethod = 'gist') : self
    {
        return new self($accessMethod);
    }

    public function deferrable(bool $initiallyDeferred = false) : self
    {
        return new self(
            $this->accessMethod,
            $this->elements,
            $this->name,
            $this->whereCondition,
            true,
            $initiallyDeferred,
        );
    }

    public function element(Expression $element, string $operator) : self
    {
        return new self(
            $this->accessMethod,
            [...$this->elements, ['element' => $element, 'operator' => $operator]],
            $this->name,
            $this->whereCondition,
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

    public function name(string $name) : self
    {
        return new self(
            $this->accessMethod,
            $this->elements,
            $name,
            $this->whereCondition,
            $this->deferrable,
            $this->initiallyDeferred,
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
                $indexElemNode = new Node();
                $indexElemNode->setIndexElem($this->createIndexElem($element['element']));

                $pair = new PBList();
                $pair->setItems([$indexElemNode, $this->createOperatorNode($element['operator'])]);

                $pairNode = new Node();
                $pairNode->setList($pair);

                $exclusions[] = $pairNode;
            }

            $constraint->setExclusions($exclusions);
        }

        if ($this->whereCondition !== null) {
            $constraint->setWhereClause($this->whereCondition->toAst());
        }

        if ($this->deferrable) {
            $constraint->setDeferrable(true);

            if ($this->initiallyDeferred) {
                $constraint->setInitdeferred(true);
            }
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
            $this->deferrable,
            $this->initiallyDeferred,
        );
    }

    private function createIndexElem(Expression $expression) : IndexElem
    {
        $indexElem = new IndexElem();

        if ($expression instanceof Column && \count($expression->parts()) === 1) {
            $indexElem->setName($expression->columnName());

            return $indexElem;
        }

        $indexElem->setExpr($expression->toAst());

        return $indexElem;
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
