<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;

/**
 * Represents an aliased expression: expr AS alias.
 */
final readonly class AliasedExpression implements Expression
{
    public function __construct(
        private Expression $expression,
        private string $alias,
    ) {
        if ($this->alias === '') {
            throw InvalidExpressionException::invalidValue('Alias', 'cannot be empty');
        }
    }

    public static function create(Expression $expression, string $alias): self
    {
        return new self($expression, $alias);
    }

    public static function fromAst(Node $node): static
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

        $aliasName = $resTarget->getName();

        if ($aliasName === null || $aliasName === '') {
            throw InvalidAstException::missingRequiredField('name', 'ResTarget');
        }

        return new self($expression, $aliasName);
    }

    public function as(string $alias): self
    {
        return new self($this->expression, $alias);
    }

    public function getAlias(): string
    {
        return $this->alias;
    }

    public function getExpression(): Expression
    {
        return $this->expression;
    }

    public function toAst(): Node
    {
        $resTarget = new ResTarget();
        $resTarget->setName($this->alias);
        $resTarget->setVal($this->expression->toAst());

        $node = new Node();
        $node->setResTarget($resTarget);

        return $node;
    }

    public function withAlias(string $alias): self
    {
        return new self($this->expression, $alias);
    }

    public function withExpression(Expression $expression): self
    {
        return new self($expression, $this->alias);
    }
}
