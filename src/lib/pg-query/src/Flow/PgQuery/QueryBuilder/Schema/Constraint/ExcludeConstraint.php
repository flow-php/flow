<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Constraint;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{ConstrType, Constraint, Node, PBString};
use Flow\PgQuery\Protobuf\AST\PBList;
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

final readonly class ExcludeConstraint implements TableConstraint
{
    /**
     * @param list<array{element: string, operator: string}> $elements
     */
    private function __construct(
        private string $accessMethod,
        private array $elements = [],
        private ?string $name = null,
        private ?string $whereClause = null,
    ) {
    }

    public static function create(string $accessMethod = 'gist') : self
    {
        return new self($accessMethod);
    }

    public function element(string $element, string $operator) : self
    {
        return new self(
            $this->accessMethod,
            [...$this->elements, ['element' => $element, 'operator' => $operator]],
            $this->name,
            $this->whereClause,
        );
    }

    public function name(string $name) : self
    {
        return new self(
            $this->accessMethod,
            $this->elements,
            $name,
            $this->whereClause,
        );
    }

    public function toAst() : Constraint
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_EXCLUSION);
        $constraint->setAccessMethod($this->accessMethod);

        if ($this->name !== null) {
            $constraint->setConname($this->name);
        }

        if ($this->elements !== []) {
            $exclusions = [];

            foreach ($this->elements as $element) {
                $exclusions[] = $this->parseExpression($element['element']);
                $exclusions[] = $this->createOperatorNode($element['operator']);
            }

            $constraint->setExclusions($exclusions);
        }

        if ($this->whereClause !== null) {
            $constraint->setWhereClause($this->parseExpression($this->whereClause));
        }

        return $constraint;
    }

    public function where(string $whereClause) : self
    {
        return new self(
            $this->accessMethod,
            $this->elements,
            $this->name,
            $whereClause,
        );
    }

    private function createOperatorNode(string $operator) : Node
    {
        $operatorList = new PBList();
        $operatorItems = [];

        $str = new PBString();
        $str->setSval($operator);

        $node = new Node();
        $node->setString($str);
        $operatorItems[] = $node;

        $operatorList->setItems($operatorItems);

        $resultNode = new Node();
        $resultNode->setList($operatorList);

        return $resultNode;
    }

    private function parseExpression(string $expression) : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT {$expression} AS x");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $firstStmt = $stmts[0];
        $selectStmt = $firstStmt->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $firstTarget = $targetList[0];
        $resTarget = $firstTarget->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $val = $resTarget->getVal();

        if ($val === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        return $val;
    }
}
