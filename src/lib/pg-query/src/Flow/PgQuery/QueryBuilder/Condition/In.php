<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{A_Expr, A_Expr_Kind, Node, PBList};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException};
use Flow\PgQuery\QueryBuilder\Expression\{Expression, ExpressionFactory};

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
    }

    public static function fromAst(Node $node) : static
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

        if ($items === null) {
            throw InvalidAstException::missingRequiredField('items', 'List');
        }

        $values = [];

        foreach ($items as $item) {
            $values[] = ExpressionFactory::fromAst($item);
        }

        return new self(
            ExpressionFactory::fromAst($lexpr),
            $values
        );
    }

    public function and(Condition $other) : AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function not() : NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other) : OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst() : Node
    {
        $valueNodes = [];

        foreach ($this->values as $value) {
            $valueNodes[] = $value->toAst();
        }

        $list = new PBList(['items' => $valueNodes]);
        $listNode = new Node(['list' => $list]);

        $aExpr = new A_Expr([
            'kind' => A_Expr_Kind::AEXPR_IN,
            'lexpr' => $this->expression->toAst(),
            'rexpr' => $listNode,
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
