<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Clause;

use Flow\PgQuery\Protobuf\AST\{Node, ResTarget};
use Flow\PgQuery\QueryBuilder\Bridge\AstConvertible;
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{Column, Expression, ExpressionFactory, Star};

/**
 * Represents a RETURNING clause for INSERT/UPDATE/DELETE statements.
 */
final readonly class ReturningClause implements AstConvertible
{
    /**
     * @param list<Expression> $expressions
     */
    public function __construct(
        private array $expressions,
    ) {
    }

    public static function all() : self
    {
        return new self([Star::all()]);
    }

    public static function columns(string ...$columns) : self
    {
        $expressions = [];

        foreach ($columns as $column) {
            $expressions[] = Column::name($column);
        }

        return new self($expressions);
    }

    public static function fromAst(Node $node) : static
    {
        $resTarget = $node->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $valNode = $resTarget->getVal();

        if ($valNode === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        $expression = ExpressionFactory::fromAst($valNode);

        return new self([$expression]);
    }

    /**
     * @return list<Node>
     */
    public static function toAstNodes(self $clause) : array
    {
        $nodes = [];

        foreach ($clause->expressions as $expr) {
            $resTarget = new ResTarget();
            $resTarget->setVal($expr->toAst());

            $node = new Node();
            $node->setResTarget($resTarget);

            $nodes[] = $node;
        }

        return $nodes;
    }

    /**
     * @return list<Expression>
     */
    public function expressions() : array
    {
        return $this->expressions;
    }

    public function toAst() : Node
    {
        if ($this->expressions === []) {
            throw InvalidAstException::missingRequiredField('expressions', 'ReturningClause');
        }

        $resTarget = new ResTarget();
        $resTarget->setVal($this->expressions[0]->toAst());

        $node = new Node();
        $node->setResTarget($resTarget);

        return $node;
    }
}
