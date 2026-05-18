<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CaseExpr;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;

use function array_values;
use function count;

final readonly class CaseExpression implements Expression
{
    /**
     * @param null|Expression $arg Expression for simple CASE (CASE arg WHEN val THEN...), null for searched CASE (CASE WHEN cond THEN...)
     * @param non-empty-list<WhenClause> $whenClauses WHEN clauses
     * @param null|Expression $elseResult ELSE clause result
     */
    public function __construct(
        private ?Expression $arg,
        private array $whenClauses,
        private ?Expression $elseResult = null,
    ) {
        if ($this->whenClauses === []) {
            throw InvalidExpressionException::emptyArray('WHEN clauses');
        }
    }

    public static function fromAst(Node $node): static
    {
        $caseExpr = $node->getCaseExpr();

        if ($caseExpr === null) {
            throw InvalidAstException::unexpectedNodeType('CaseExpr', 'unknown');
        }

        $argNode = $caseExpr->getArg();
        $arg = $argNode !== null ? ExpressionFactory::fromAst($argNode) : null;

        $argsNodes = $caseExpr->getArgs();

        if (count($argsNodes) === 0) {
            throw InvalidAstException::missingRequiredField('args', 'CaseExpr');
        }

        $whenClauses = [];

        foreach ($argsNodes as $whenNode) {
            $whenClauses[] = WhenClause::fromAst($whenNode);
        }

        if ($whenClauses === []) {
            throw InvalidAstException::invalidFieldValue('args', 'CaseExpr', 'cannot be empty');
        }

        $defResultNode = $caseExpr->getDefresult();
        $elseResult = $defResultNode !== null ? ExpressionFactory::fromAst($defResultNode) : null;

        return new self($arg, $whenClauses, $elseResult);
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function getArg(): ?Expression
    {
        return $this->arg;
    }

    public function getElseResult(): ?Expression
    {
        return $this->elseResult;
    }

    /**
     * @return non-empty-list<WhenClause>
     */
    public function getWhenClauses(): array
    {
        return $this->whenClauses;
    }

    public function toAst(): Node
    {
        $caseExpr = new CaseExpr();

        if ($this->arg !== null) {
            $caseExpr->setArg($this->arg->toAst());
        }

        $whenNodes = [];

        foreach ($this->whenClauses as $whenClause) {
            $whenNodes[] = $whenClause->toAst();
        }

        $caseExpr->setArgs($whenNodes);

        if ($this->elseResult !== null) {
            $caseExpr->setDefresult($this->elseResult->toAst());
        }

        $node = new Node();
        $node->setCaseExpr($caseExpr);

        return $node;
    }

    public function withElse(Expression $elseResult): self
    {
        return new self($this->arg, $this->whenClauses, $elseResult);
    }

    public function withWhen(WhenClause ...$whenClauses): self
    {
        $clauses = array_values($whenClauses);

        if ($clauses === []) {
            throw InvalidExpressionException::emptyArray('WHEN clauses');
        }

        return new self($this->arg, $clauses, $this->elseResult);
    }
}
