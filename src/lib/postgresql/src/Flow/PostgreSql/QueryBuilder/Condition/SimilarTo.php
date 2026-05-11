<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\CoercionForm;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;

final readonly class SimilarTo implements Condition
{
    public function __construct(
        public Expression $expression,
        public Expression $pattern,
        public ?Expression $escape = null,
    ) {}

    public static function fromAst(Node $node): static
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        $kind = $aExpr->getKind();

        if ($kind !== A_Expr_Kind::AEXPR_SIMILAR) {
            throw InvalidAstException::invalidFieldValue(
                'kind',
                'A_Expr',
                'Expected AEXPR_SIMILAR for SimilarTo condition',
            );
        }

        $lexpr = $aExpr->getLexpr();

        if ($lexpr === null) {
            throw InvalidAstException::missingRequiredField('lexpr', 'A_Expr');
        }

        $rexpr = $aExpr->getRexpr();

        if ($rexpr === null) {
            throw InvalidAstException::missingRequiredField('rexpr', 'A_Expr');
        }

        $pattern = null;
        $escape = null;

        if ($rexpr->hasFuncCall()) {
            $funcCall = $rexpr->getFuncCall();
            $args = $funcCall?->getArgs();

            if ($args !== null && \count($args) > 0) {
                $pattern = ExpressionFactory::fromAst($args[0]);
            }

            if ($args !== null && \count($args) > 1) {
                $escape = ExpressionFactory::fromAst($args[1]);
            }
        } else {
            $pattern = ExpressionFactory::fromAst($rexpr);
        }

        if ($pattern === null) {
            throw InvalidAstException::missingRequiredField('pattern', 'SimilarTo');
        }

        return new self(ExpressionFactory::fromAst($lexpr), $pattern, $escape);
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
        $operatorString = new PBString(['sval' => '~']);
        $operatorNode = new Node(['string' => $operatorString]);

        $funcArgs = [$this->pattern->toAst()];

        if ($this->escape !== null) {
            $funcArgs[] = $this->escape->toAst();
        }

        $funcCall = new FuncCall([
            'funcname' => [
                new Node(['string' => new PBString(['sval' => 'pg_catalog'])]),
                new Node(['string' => new PBString(['sval' => 'similar_to_escape'])]),
            ],
            'args' => $funcArgs,
            'funcformat' => CoercionForm::COERCE_EXPLICIT_CALL,
            'location' => -1,
        ]);

        $aExpr = new A_Expr([
            'kind' => A_Expr_Kind::AEXPR_SIMILAR,
            'name' => [$operatorNode],
            'lexpr' => $this->expression->toAst(),
            'rexpr' => new Node(['func_call' => $funcCall]),
            'location' => -1,
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
