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

final readonly class Between implements Condition
{
    public function __construct(
        public Expression $expression,
        public Expression $low,
        public Expression $high,
        public bool $symmetric = false,
        public bool $negated = false,
    ) {}

    public static function fromAst(Node $node): static
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        $kind = $aExpr->getKind();

        if (
            $kind !== A_Expr_Kind::AEXPR_BETWEEN
            && $kind !== A_Expr_Kind::AEXPR_NOT_BETWEEN
            && $kind !== A_Expr_Kind::AEXPR_BETWEEN_SYM
            && $kind !== A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM
        ) {
            throw InvalidAstException::invalidFieldValue(
                'kind',
                'A_Expr',
                'Expected BETWEEN variant for Between condition',
            );
        }

        $symmetric = $kind === A_Expr_Kind::AEXPR_BETWEEN_SYM || $kind === A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM;
        $negated = $kind === A_Expr_Kind::AEXPR_NOT_BETWEEN || $kind === A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM;

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

        if ($items->count() !== 2) {
            throw InvalidAstException::invalidFieldValue(
                'rexpr',
                'A_Expr',
                'Expected List with exactly 2 items for BETWEEN',
            );
        }

        return new self(
            ExpressionFactory::fromAst($lexpr),
            ExpressionFactory::fromAst($items->offsetGet(0)),
            ExpressionFactory::fromAst($items->offsetGet(1)),
            $symmetric,
            $negated,
        );
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
        $kind = match (true) {
            $this->symmetric && $this->negated => A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM,
            $this->symmetric => A_Expr_Kind::AEXPR_BETWEEN_SYM,
            $this->negated => A_Expr_Kind::AEXPR_NOT_BETWEEN,
            default => A_Expr_Kind::AEXPR_BETWEEN,
        };

        $list = new PBList([
            'items' => [
                $this->low->toAst(),
                $this->high->toAst(),
            ],
        ]);

        $listNode = new Node(['list' => $list]);

        $nameString = new PBString();
        $nameString->setSval('BETWEEN');
        $nameNode = new Node(['string' => $nameString]);

        $aExpr = new A_Expr([
            'kind' => $kind,
            'name' => [$nameNode],
            'lexpr' => $this->expression->toAst(),
            'rexpr' => $listNode,
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
