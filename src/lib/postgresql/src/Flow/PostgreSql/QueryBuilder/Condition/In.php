<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use InvalidArgumentException;

final readonly class In implements Condition
{
    /**
     * @param Expression $expression
     * @param array<Expression> $values
     */
    public function __construct(
        public Expression $expression,
        public array $values,
    ) {
        if ($values === []) {
            throw new InvalidArgumentException('IN condition requires at least 1 value');
        }
    }

    public static function fromAst(Node $node): static
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        if ($aExpr->getKind() !== A_Expr_Kind::AEXPR_IN) {
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'Expected AEXPR_IN for In condition');
        }

        $lexpr = $aExpr->getLexpr();

        if ($lexpr === null) {
            throw InvalidAstException::missingRequiredField('lexpr', 'A_Expr');
        }

        $rexpr = $aExpr->getRexpr();

        if ($rexpr === null) {
            throw InvalidAstException::missingRequiredField('rexpr', 'A_Expr');
        }

        $list = $rexpr->getList();

        if ($list === null) {
            throw InvalidAstException::unexpectedNodeType('List', 'unknown');
        }

        $items = $list->getItems();

        $values = [];

        foreach ($items as $item) {
            $values[] = ExpressionFactory::fromAst($item);
        }

        return new self(ExpressionFactory::fromAst($lexpr), $values);
    }

    public function and(Condition $other): AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function not(): NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other): OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst(): Node
    {
        $valueNodes = [];

        foreach ($this->values as $value) {
            $valueNodes[] = $value->toAst();
        }

        $list = new PBList(['items' => $valueNodes]);
        $listNode = new Node(['list' => $list]);

        $nameString = new PBString();
        $nameString->setSval('=');
        $nameNode = new Node(['string' => $nameString]);

        $aExpr = new A_Expr([
            'kind' => A_Expr_Kind::AEXPR_IN,
            'name' => [$nameNode],
            'lexpr' => $this->expression->toAst(),
            'rexpr' => $listNode,
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
