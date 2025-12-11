<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{Node, ParamRef};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};

/**
 * Represents a positional parameter in SQL (e.g., $1, $2, $3).
 */
final readonly class Parameter implements Expression
{
    private function __construct(
        private int $number,
    ) {
        if ($number < 1) {
            throw InvalidExpressionException::invalidValue('Parameter number', $number);
        }
    }

    public static function fromAst(Node $node) : static
    {
        $paramRef = $node->getParamRef();

        if ($paramRef === null) {
            throw InvalidAstException::unexpectedNodeType('ParamRef', 'unknown');
        }

        $number = $paramRef->getNumber();

        if ($number < 1) {
            throw InvalidAstException::invalidFieldValue('number', 'ParamRef', 'Must be >= 1');
        }

        return new self($number);
    }

    /**
     * Create a parameter reference with the given position number.
     */
    public static function positional(int $number) : self
    {
        return new self($number);
    }

    public function as(string $alias) : AliasedExpression
    {
        return AliasedExpression::create($this, $alias);
    }

    public function number() : int
    {
        return $this->number;
    }

    public function toAst() : Node
    {
        $paramRef = new ParamRef();
        $paramRef->setNumber($this->number);

        $node = new Node();
        $node->setParamRef($paramRef);

        return $node;
    }
}
