<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\{ConstrType, Constraint, Node, ParseResult, RawStmt, ResTarget, SelectStmt, SetOperation};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ExcludeDefinitionParser
{
    private const string EXPR_ALIAS = '__flow_expr';

    public function __construct(
        private Parser $parser,
        private ExpressionParser $expressionParser,
    ) {
    }

    public function parse(string $definition) : ParsedExcludeDefinition
    {
        $trimmed = \ltrim($definition);

        if (\stripos($trimmed, 'EXCLUDE') !== 0) {
            $trimmed = 'EXCLUDE ' . $trimmed;
        }

        $parsed = $this->parser->parse('CREATE TABLE __flow_exclude_norm (' . $trimmed . ')');
        $constraint = $this->extractConstraint($parsed->raw());

        return new ParsedExcludeDefinition(
            accessMethod: \strtolower($constraint->getAccessMethod()),
            elements: $this->extractElements($constraint),
            predicate: $this->extractPredicate($constraint),
            deferrable: $constraint->getDeferrable(),
            initiallyDeferred: $constraint->getInitdeferred(),
        );
    }

    private function deparseExpressionNode(Node $expression) : string
    {
        $resTarget = new ResTarget();
        $resTarget->setName(self::EXPR_ALIAS);
        $resTarget->setVal($expression);

        $resTargetNode = new Node();
        $resTargetNode->setResTarget($resTarget);

        $selectStmt = new SelectStmt();
        $selectStmt->setTargetList([$resTargetNode]);
        $selectStmt->setOp(SetOperation::SETOP_NONE);

        $stmtNode = new Node();
        $stmtNode->setSelectStmt($selectStmt);

        $rawStmt = new RawStmt();
        $rawStmt->setStmt($stmtNode);

        $parseResult = new ParseResult();
        $parseResult->setVersion(170007);
        $parseResult->setStmts([$rawStmt]);

        $sql = \pg_query_deparse($parseResult->serializeToString());
        $suffixLength = \strlen(' AS ' . self::EXPR_ALIAS);

        return \substr($sql, 7, -$suffixLength);
    }

    private function extractConstraint(ParseResult $parseResult) : Constraint
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

        throw InvalidAstException::invalidFieldValue('tableElts', 'CreateStmt', 'no EXCLUDE constraint found in definition');
    }

    /**
     * @return list<array{expression: string, operator: string}>
     */
    private function extractElements(Constraint $constraint) : array
    {
        $elements = [];

        foreach ($constraint->getExclusions() as $exclusionNode) {
            $pair = $exclusionNode->getList();

            if ($pair === null) {
                throw InvalidAstException::unexpectedNodeType('List', 'unknown');
            }

            $items = \iterator_to_array($pair->getItems());

            if (\count($items) !== 2) {
                throw InvalidAstException::invalidFieldValue('items', 'List', 'expected exactly 2 items in exclusion pair');
            }

            $elements[] = [
                'expression' => $this->resolveElementExpression($items[0]),
                'operator' => $this->resolveOperator($items[1]),
            ];
        }

        return $elements;
    }

    private function extractPredicate(Constraint $constraint) : ?string
    {
        $where = $constraint->getWhereClause();

        if ($where === null) {
            return null;
        }

        return $this->expressionParser->normalize($this->deparseExpressionNode($where));
    }

    private function resolveElementExpression(Node $elementNode) : string
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
            throw InvalidAstException::invalidFieldValue('expr', 'IndexElem', 'element has neither name nor expression');
        }

        return $this->expressionParser->normalize($this->deparseExpressionNode($expr));
    }

    private function resolveOperator(Node $operatorNode) : string
    {
        $list = $operatorNode->getList();

        if ($list === null) {
            throw InvalidAstException::unexpectedNodeType('List', 'unknown');
        }

        $items = \iterator_to_array($list->getItems());

        if (\count($items) === 0) {
            throw InvalidAstException::invalidFieldValue('items', 'List', 'operator list is empty');
        }

        $string = $items[0]->getString();

        if ($string === null) {
            throw InvalidAstException::unexpectedNodeType('String', 'unknown');
        }

        return $string->getSval();
    }
}
