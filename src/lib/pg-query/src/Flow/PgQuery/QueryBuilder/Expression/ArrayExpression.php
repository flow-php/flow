<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{A_ArrayExpr, Node};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

/**
 * ARRAY[expr, expr, ...] - PostgreSQL array constructor.
 */
final readonly class ArrayExpression implements Expression
{
    /**
     * @param array<Expression> $elements
     */
    public function __construct(
        private array $elements,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $arrayExpr = $node->getAArrayExpr();

        if ($arrayExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_ArrayExpr', 'unknown');
        }

        $elements = $arrayExpr->getElements();

        $expressions = [];

        if ($elements !== null) {
            foreach ($elements as $elementNode) {
                $expressions[] = self::expressionFromNode($elementNode);
            }
        }

        return new self($expressions);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    /**
     * @return array<Expression>
     */
    public function elements() : array
    {
        return $this->elements;
    }

    public function toAst() : Node
    {
        $elementNodes = [];

        foreach ($this->elements as $element) {
            $elementNodes[] = $element->toAst();
        }

        $arrayExpr = new A_ArrayExpr();
        $arrayExpr->setElements($elementNodes);
        $arrayExpr->setLocation(-1);

        $node = new Node();
        $node->setAArrayExpr($arrayExpr);

        return $node;
    }

    private static function expressionFromNode(Node $node) : Expression
    {
        return ExpressionFactory::fromAst($node);
    }
}
