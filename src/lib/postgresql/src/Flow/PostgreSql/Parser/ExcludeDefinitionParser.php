<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ExcludeDefinitionParser
{
    public function __construct(
        private ExpressionParser $expressionParser,
    ) {}

    public function parse(string $definition): ParsedExcludeDefinition
    {
        $trimmed = \ltrim($definition);

        if (\stripos($trimmed, 'EXCLUDE') !== 0) {
            $trimmed = 'EXCLUDE ' . $trimmed;
        }

        $parsed = $this->expressionParser->parseStatement('CREATE TABLE __flow_exclude_norm (' . $trimmed . ')');
        $constraint = $this->extractConstraint($parsed->raw());

        return new ParsedExcludeDefinition(
            accessMethod: \strtolower($constraint->getAccessMethod()),
            elements: $this->extractElements($constraint),
            predicate: $this->extractPredicate($constraint),
            deferrable: $constraint->getDeferrable(),
            initiallyDeferred: $constraint->getInitdeferred(),
        );
    }

    private function extractConstraint(ParseResult $parseResult): Constraint
    {
        $stmts = $parseResult->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $createStmt = $stmts[0]->getStmt()?->getCreateStmt();

        if ($createStmt === null) {
            throw InvalidAstException::unexpectedNodeType('CreateStmt', 'unknown');
        }

        foreach ($createStmt->getTableElts() as $elt) {
            $constraint = $elt->getConstraint();

            if ($constraint !== null && $constraint->getContype() === ConstrType::CONSTR_EXCLUSION) {
                return $constraint;
            }
        }

        throw InvalidAstException::invalidFieldValue(
            'tableElts',
            'CreateStmt',
            'no EXCLUDE constraint found in definition',
        );
    }

    /**
     * @return list<array{expression: string, operator: string}>
     */
    private function extractElements(Constraint $constraint): array
    {
        $elements = [];

        foreach ($constraint->getExclusions() as $exclusionNode) {
            $pair = $exclusionNode->getList();

            if ($pair === null) {
                throw InvalidAstException::unexpectedNodeType('List', 'unknown');
            }

            if ($pair->getItems()->count() !== 2) {
                throw InvalidAstException::invalidFieldValue(
                    'items',
                    'List',
                    'expected exactly 2 items in exclusion pair',
                );
            }

            $elements[] = [
                'expression' => $this->resolveElementExpression($pair->getItems()->offsetGet(0)),
                'operator' => $this->resolveOperator($pair->getItems()->offsetGet(1)),
            ];
        }

        return $elements;
    }

    private function extractPredicate(Constraint $constraint): ?string
    {
        $where = $constraint->getWhereClause();

        if ($where === null) {
            return null;
        }

        return $this->expressionParser->normalizeNode($where);
    }

    private function resolveElementExpression(Node $elementNode): string
    {
        $indexElem = $elementNode->getIndexElem();

        if ($indexElem === null) {
            throw InvalidAstException::unexpectedNodeType('IndexElem', 'unknown');
        }

        $name = $indexElem->getName();

        if ($name !== '') {
            return $this->expressionParser->normalize($name);
        }

        $expr = $indexElem->getExpr();

        if ($expr === null) {
            throw InvalidAstException::invalidFieldValue(
                'expr',
                'IndexElem',
                'element has neither name nor expression',
            );
        }

        return $this->expressionParser->normalizeNode($expr);
    }

    private function resolveOperator(Node $operatorNode): string
    {
        $list = $operatorNode->getList();

        if ($list === null) {
            throw InvalidAstException::unexpectedNodeType('List', 'unknown');
        }

        if ($list->getItems()->count() === 0) {
            throw InvalidAstException::invalidFieldValue('items', 'List', 'operator list is empty');
        }

        $string = $list->getItems()->offsetGet(0)->getString();

        if ($string === null) {
            throw InvalidAstException::unexpectedNodeType('String', 'unknown');
        }

        return $string->getSval();
    }
}
