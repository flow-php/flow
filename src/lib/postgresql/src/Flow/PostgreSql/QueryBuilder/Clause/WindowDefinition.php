<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{Node, WindowDef};
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Expression, ExpressionFactory};

/**
 * Represents a named window definition for WINDOW clause.
 */
final readonly class WindowDefinition implements AstConvertible
{
    /**
     * @param list<Expression> $partitionBy
     * @param list<OrderBy|OrderByItem> $orderBy
     */
    public function __construct(
        private string $name,
        private array $partitionBy = [],
        private array $orderBy = [],
        private ?WindowFrame $frame = null,
        private ?string $refName = null,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $windowDef = $node->getWindowDef();

        if ($windowDef === null) {
            throw InvalidAstException::unexpectedNodeType('WindowDef', 'unknown');
        }

        $name = $windowDef->getName();
        $refName = $windowDef->getRefname();

        if ($refName === '') {
            $refName = null;
        }

        $partitionBy = [];
        $partitionClause = $windowDef->getPartitionClause();

        if ($partitionClause !== null) {
            foreach ($partitionClause as $partNode) {
                $partitionBy[] = ExpressionFactory::fromAst($partNode);
            }
        }

        $orderBy = [];
        $orderClause = $windowDef->getOrderClause();

        if ($orderClause !== null) {
            foreach ($orderClause as $orderNode) {
                $orderBy[] = OrderBy::fromAst($orderNode);
            }
        }

        $frame = null;

        if ($windowDef->getFrameOptions() !== 0) {
            $frame = WindowFrame::fromAst($node);
        }

        return new self($name, $partitionBy, $orderBy, $frame, $refName);
    }

    public function frame() : ?WindowFrame
    {
        return $this->frame;
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @return list<OrderBy|OrderByItem>
     */
    public function orderBy() : array
    {
        return $this->orderBy;
    }

    /**
     * @return list<Expression>
     */
    public function partitionBy() : array
    {
        return $this->partitionBy;
    }

    public function refName() : ?string
    {
        return $this->refName;
    }

    public function toAst() : Node
    {
        $windowDef = new WindowDef();
        $windowDef->setName($this->name);

        if ($this->refName !== null) {
            $windowDef->setRefname($this->refName);
        }

        if ($this->partitionBy !== []) {
            $partitionNodes = [];

            foreach ($this->partitionBy as $expr) {
                $partitionNodes[] = $expr->toAst();
            }

            $windowDef->setPartitionClause($partitionNodes);
        }

        if ($this->orderBy !== []) {
            $orderNodes = [];

            foreach ($this->orderBy as $orderItem) {
                if ($orderItem instanceof OrderByItem) {
                    $orderNodes[] = new Node(['sort_by' => $orderItem->toAst()]);
                } else {
                    $orderNodes[] = $orderItem->toAst();
                }
            }

            $windowDef->setOrderClause($orderNodes);
        }

        if ($this->frame !== null) {
            $windowDef->setFrameOptions(0);

            if ($this->frame->startBound()->offset() !== null) {
                $windowDef->setStartOffset($this->frame->startBound()->toAst());
            }

            if ($this->frame->endBound() !== null && $this->frame->endBound()->offset() !== null) {
                $windowDef->setEndOffset($this->frame->endBound()->toAst());
            }
        }

        $node = new Node();
        $node->setWindowDef($windowDef);

        return $node;
    }

    public function withFrame(?WindowFrame $frame) : self
    {
        return new self($this->name, $this->partitionBy, $this->orderBy, $frame, $this->refName);
    }

    /**
     * @param list<OrderBy> $orderBy
     */
    public function withOrderBy(array $orderBy) : self
    {
        return new self($this->name, $this->partitionBy, $orderBy, $this->frame, $this->refName);
    }

    /**
     * @param list<Expression> $partitionBy
     */
    public function withPartitionBy(array $partitionBy) : self
    {
        return new self($this->name, $partitionBy, $this->orderBy, $this->frame, $this->refName);
    }
}
