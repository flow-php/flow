<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\TypeCast as AstTypeCast;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

/**
 * Represents a type cast expression: expr::type or CAST(expr AS type).
 */
final readonly class TypeCast implements Expression
{
    /**
     * @param Expression $expression Expression to cast
     * @param ColumnType $dataType Target data type
     */
    public function __construct(
        private Expression $expression,
        private ColumnType $dataType,
    ) {}

    public static function fromAst(Node $node): static
    {
        $typeCast = $node->getTypeCast();

        if ($typeCast === null) {
            throw InvalidAstException::unexpectedNodeType('TypeCast', 'unknown');
        }

        $argNode = $typeCast->getArg();

        if ($argNode === null) {
            throw InvalidAstException::missingRequiredField('arg', 'TypeCast');
        }

        $expression = ExpressionFactory::fromAst($argNode);

        $typeNameNode = $typeCast->getTypeName();

        if ($typeNameNode === null) {
            throw InvalidAstException::missingRequiredField('typeName', 'TypeCast');
        }

        $dataType = ColumnType::fromAst($typeNameNode);

        return new self($expression, $dataType);
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function getColumnType(): ColumnType
    {
        return $this->dataType;
    }

    public function getExpression(): Expression
    {
        return $this->expression;
    }

    public function toAst(): Node
    {
        $typeCast = new AstTypeCast();
        $typeCast->setArg($this->expression->toAst());
        $typeCast->setTypeName($this->dataType->toAst());

        $node = new Node();
        $node->setTypeCast($typeCast);

        return $node;
    }

    public function withColumnType(ColumnType $dataType): self
    {
        return new self($this->expression, $dataType);
    }

    public function withExpression(Expression $expression): self
    {
        return new self($expression, $this->dataType);
    }
}
