<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CoercionForm;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RowExpr;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * ROW(expr, expr, ...) or (expr, expr, ...) - PostgreSQL row constructor.
 */
final readonly class RowExpression implements Expression
{
    /**
     * @param array<Expression> $args
     * @param bool $explicitRow Whether to use ROW keyword (true) or just parentheses (false)
     */
    public function __construct(
        private array $args,
        private bool $explicitRow = false,
    ) {
        if (\count($this->args) === 0) {
            throw new \InvalidArgumentException('RowExpression requires at least 1 expression');
        }
    }

    public static function fromAst(Node $node): static
    {
        $rowExpr = $node->getRowExpr();

        if ($rowExpr === null) {
            throw InvalidAstException::unexpectedNodeType('RowExpr', 'unknown');
        }

        $args = $rowExpr->getArgs();

        if (\count($args) === 0) {
            throw InvalidAstException::invalidFieldValue('args', 'RowExpr', 'must have at least 1 argument');
        }

        $expressions = [];

        foreach ($args as $argNode) {
            $expressions[] = self::expressionFromNode($argNode);
        }

        $rowFormat = $rowExpr->getRowFormat();
        $explicitRow = $rowFormat === CoercionForm::COERCE_EXPLICIT_CALL;

        return new self($expressions, $explicitRow);
    }

    /**
     * @return array<Expression>
     */
    public function args(): array
    {
        return $this->args;
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function isExplicitRow(): bool
    {
        return $this->explicitRow;
    }

    public function toAst(): Node
    {
        $argNodes = [];

        foreach ($this->args as $arg) {
            $argNodes[] = $arg->toAst();
        }

        $rowExpr = new RowExpr();
        $rowExpr->setArgs($argNodes);
        $rowExpr->setRowFormat(
            $this->explicitRow ? CoercionForm::COERCE_EXPLICIT_CALL : CoercionForm::COERCE_IMPLICIT_CAST,
        );
        $rowExpr->setRowTypeid(0);
        $rowExpr->setLocation(-1);

        $node = new Node();
        $node->setRowExpr($rowExpr);

        return $node;
    }

    private static function expressionFromNode(Node $node): Expression
    {
        return ExpressionFactory::fromAst($node);
    }
}
