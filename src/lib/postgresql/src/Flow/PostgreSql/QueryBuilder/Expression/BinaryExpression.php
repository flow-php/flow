<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Binary expression: left op right (e.g., a + b, x * y, s1 || s2).
 */
final readonly class BinaryExpression implements Expression
{
    public function __construct(
        private Expression $left,
        private string $operator,
        private Expression $right,
    ) {}

    public static function fromAst(Node $node): static
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        if ($aExpr->getKind() !== A_Expr_Kind::AEXPR_OP) {
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'must be AEXPR_OP for binary expression');
        }

        $lexpr = $aExpr->getLexpr();
        $rexpr = $aExpr->getRexpr();

        if ($lexpr === null) {
            throw InvalidAstException::missingRequiredField('lexpr', 'A_Expr');
        }

        if ($rexpr === null) {
            throw InvalidAstException::missingRequiredField('rexpr', 'A_Expr');
        }

        $name = $aExpr->getName();

        if (\count($name) === 0) {
            throw InvalidAstException::missingRequiredField('name', 'A_Expr');
        }

        $operatorNode = $name[\count($name) - 1];
        $operatorString = $operatorNode->getString();

        if ($operatorString === null) {
            throw InvalidAstException::invalidFieldValue('name', 'A_Expr', 'operator must be a String node');
        }

        return new self(
            ExpressionFactory::fromAst($lexpr),
            $operatorString->getSval(),
            ExpressionFactory::fromAst($rexpr),
        );
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function left(): Expression
    {
        return $this->left;
    }

    public function operator(): string
    {
        return $this->operator;
    }

    public function right(): Expression
    {
        return $this->right;
    }

    public function toAst(): Node
    {
        $operatorNode = new Node();
        $operatorString = new PBString();
        $operatorString->setSval($this->operator);
        $operatorNode->setString($operatorString);

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);
        $aExpr->setName([$operatorNode]);
        $aExpr->setLexpr($this->left->toAst());
        $aExpr->setRexpr($this->right->toAst());
        $aExpr->setLocation(-1);

        $node = new Node();
        $node->setAExpr($aExpr);

        return $node;
    }
}
