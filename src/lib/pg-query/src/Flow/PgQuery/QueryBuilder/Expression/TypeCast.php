<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{Node, PBString, TypeCast as AstTypeCast, TypeName};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};

/**
 * Represents a type cast expression: expr::type or CAST(expr AS type).
 */
final readonly class TypeCast implements Expression
{
    /**
     * @param Expression $expression Expression to cast
     * @param non-empty-list<string> $typeName Type name parts (e.g., ['pg_catalog', 'int4'] or ['varchar'])
     */
    public function __construct(
        private Expression $expression,
        private array $typeName,
    ) {
        if ($this->typeName === []) {
            throw InvalidExpressionException::emptyArray('Type name');
        }
    }

    public static function fromAst(Node $node) : static
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

        $namesNodes = $typeNameNode->getNames();

        if ($namesNodes === null || \count($namesNodes) === 0) {
            throw InvalidAstException::missingRequiredField('names', 'TypeName');
        }

        $typeName = [];

        foreach ($namesNodes as $nameNode) {
            $stringNode = $nameNode->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue('names', 'TypeName', 'expected String node');
            }

            $typeName[] = $stringNode->getSval();
        }

        if ($typeName === []) {
            throw InvalidAstException::invalidFieldValue('names', 'TypeName', 'cannot be empty');
        }

        return new self($expression, $typeName);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function getExpression() : Expression
    {
        return $this->expression;
    }

    /**
     * @return non-empty-list<string>
     */
    public function getTypeName() : array
    {
        return $this->typeName;
    }

    public function toAst() : Node
    {
        $typeName = new TypeName();
        $namesNodes = [];

        foreach ($this->typeName as $namePart) {
            $stringNode = new PBString();
            $stringNode->setSval($namePart);

            $nameNode = new Node();
            $nameNode->setString($stringNode);

            $namesNodes[] = $nameNode;
        }

        $typeName->setNames($namesNodes);

        $typeCast = new AstTypeCast();
        $typeCast->setArg($this->expression->toAst());
        $typeCast->setTypeName($typeName);

        $node = new Node();
        $node->setTypeCast($typeCast);

        return $node;
    }

    public function withExpression(Expression $expression) : self
    {
        return new self($expression, $this->typeName);
    }

    public function withTypeName(string ...$typeName) : self
    {
        $typeNameList = \array_values($typeName);

        if ($typeNameList === []) {
            throw InvalidExpressionException::emptyArray('Type name');
        }

        return new self($this->expression, $typeNameList);
    }
}
