<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{Node, OnConflictClause as ProtobufOnConflictClause, ResTarget};
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Condition\{Condition, ConditionFactory};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Expression, ExpressionFactory};

/**
 * Represents an ON CONFLICT clause for INSERT statements.
 */
final readonly class OnConflictClause implements AstConvertible
{
    /**
     * @param array<string, Expression> $updates
     */
    public function __construct(
        private ConflictAction $action,
        private ?ConflictTarget $target = null,
        private array $updates = [],
        private ?Condition $whereClause = null,
    ) {
    }

    public static function doNothing(?ConflictTarget $target = null) : self
    {
        return new self(ConflictAction::NOTHING, $target);
    }

    /**
     * @param array<string, Expression> $updates
     */
    public static function doUpdate(ConflictTarget $target, array $updates) : self
    {
        return new self(ConflictAction::UPDATE, $target, $updates);
    }

    public static function fromAst(Node $node) : static
    {
        $onConflictClause = $node->getOnConflictClause();

        if ($onConflictClause === null) {
            throw InvalidAstException::unexpectedNodeType('OnConflictClause', 'unknown');
        }

        $action = ConflictAction::fromProtobuf($onConflictClause->getAction());

        $target = null;
        $inferNode = $onConflictClause->getInfer();

        if ($inferNode !== null) {
            $inferWrapperNode = new Node();
            $inferWrapperNode->setInferClause($inferNode);
            $target = ConflictTarget::fromAst($inferWrapperNode);
        }

        $updates = [];
        $targetList = $onConflictClause->getTargetList();

        if ($targetList !== null) {
            foreach ($targetList as $targetNode) {
                $resTarget = $targetNode->getResTarget();

                if ($resTarget !== null) {
                    $name = $resTarget->getName();
                    $valNode = $resTarget->getVal();

                    if ($name !== '' && $valNode !== null) {
                        $updates[$name] = ExpressionFactory::fromAst($valNode);
                    }
                }
            }
        }

        $whereClause = null;
        $whereNode = $onConflictClause->getWhereClause();

        if ($whereNode !== null) {
            $whereClause = ConditionFactory::fromAst($whereNode);
        }

        return new self($action, $target, $updates, $whereClause);
    }

    public function action() : ConflictAction
    {
        return $this->action;
    }

    public function target() : ?ConflictTarget
    {
        return $this->target;
    }

    public function toAst() : Node
    {
        $onConflictClause = new ProtobufOnConflictClause();
        $onConflictClause->setAction($this->action->toProtobuf());

        if ($this->target !== null) {
            $targetNode = $this->target->toAst();
            $inferClause = $targetNode->getInferClause();

            if ($inferClause !== null) {
                $onConflictClause->setInfer($inferClause);
            }
        }

        if ($this->updates !== []) {
            $targetList = [];

            foreach ($this->updates as $column => $expr) {
                $resTarget = new ResTarget();
                $resTarget->setName($column);
                $resTarget->setVal($expr->toAst());

                $targetNode = new Node();
                $targetNode->setResTarget($resTarget);

                $targetList[] = $targetNode;
            }

            $onConflictClause->setTargetList($targetList);
        }

        if ($this->whereClause !== null) {
            $onConflictClause->setWhereClause($this->whereClause->toAst());
        }

        $node = new Node();
        $node->setOnConflictClause($onConflictClause);

        return $node;
    }

    /**
     * @return array<string, Expression>
     */
    public function updates() : array
    {
        return $this->updates;
    }

    public function where(Condition $condition) : self
    {
        return new self($this->action, $this->target, $this->updates, $condition);
    }

    public function whereClause() : ?Condition
    {
        return $this->whereClause;
    }
}
