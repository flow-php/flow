<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CollateClause;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

use function array_map;

final readonly class Collate implements Expression
{
    private QualifiedIdentifier $collation;

    public function __construct(
        private Expression $expression,
        string|QualifiedIdentifier $collation,
    ) {
        $this->collation = $collation instanceof QualifiedIdentifier
            ? $collation
            : QualifiedIdentifier::parse($collation);
    }

    public static function fromAst(Node $node): static
    {
        $collateClause = $node->getCollateClause();

        if ($collateClause === null) {
            throw InvalidAstException::unexpectedNodeType('CollateClause', 'unknown');
        }

        $argNode = $collateClause->getArg();

        if ($argNode === null) {
            throw InvalidAstException::missingRequiredField('arg', 'CollateClause');
        }

        $parts = [];

        foreach ($collateClause->getCollname() as $nameNode) {
            $stringNode = $nameNode->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue('collname', 'CollateClause', 'expected String node');
            }

            $parts[] = $stringNode->getSval();
        }

        if ($parts === []) {
            throw InvalidAstException::missingRequiredField('collname', 'CollateClause');
        }

        return new self(ExpressionFactory::fromAst($argNode), QualifiedIdentifier::fromParts($parts));
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function getCollation(): QualifiedIdentifier
    {
        return $this->collation;
    }

    public function getExpression(): Expression
    {
        return $this->expression;
    }

    public function toAst(): Node
    {
        $collateClause = new CollateClause();
        $collateClause->setArg($this->expression->toAst());
        $collateClause->setCollname(array_map(
            static fn(string $part): Node => (new Node())->setString((new PBString())->setSval($part)),
            $this->collation->parts(),
        ));

        return (new Node())->setCollateClause($collateClause);
    }
}
