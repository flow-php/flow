<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{CaseWhen, Node};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

final readonly class WhenClause
{
    public function __construct(
        private Expression $condition,
        private Expression $result,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $caseWhen = $node->getCaseWhen();

        if ($caseWhen === null) {
            throw InvalidAstException::unexpectedNodeType('CaseWhen', 'unknown');
        }

        $exprNode = $caseWhen->getExpr();

        if ($exprNode === null) {
            throw InvalidAstException::missingRequiredField('expr', 'CaseWhen');
        }

        $resultNode = $caseWhen->getResult();

        if ($resultNode === null) {
            throw InvalidAstException::missingRequiredField('result', 'CaseWhen');
        }

        $condition = ExpressionFactory::fromAst($exprNode);
        $result = ExpressionFactory::fromAst($resultNode);

        return new self($condition, $result);
    }

    public function getCondition() : Expression
    {
        return $this->condition;
    }

    public function getResult() : Expression
    {
        return $this->result;
    }

    public function toAst() : Node
    {
        $caseWhen = new CaseWhen();
        $caseWhen->setExpr($this->condition->toAst());
        $caseWhen->setResult($this->result->toAst());

        $node = new Node();
        $node->setCaseWhen($caseWhen);

        return $node;
    }
}
