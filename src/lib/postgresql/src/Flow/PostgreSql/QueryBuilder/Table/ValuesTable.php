<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SetOperation;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\RowExpression;

/**
 * Represents a VALUES clause as a table reference: (VALUES (expr, expr), (expr, expr)).
 *
 * Usage:
 *   values_table(
 *       row(literal(1), literal('Alice')),
 *       row(literal(2), literal('Bob'))
 *   )->as('t', ['id', 'name'])
 */
final readonly class ValuesTable implements TableReference
{
    /**
     * @param array<RowExpression> $rows
     */
    public function __construct(
        private array $rows,
    ) {
        if (\count($this->rows) === 0) {
            throw new \InvalidArgumentException('ValuesTable requires at least 1 row');
        }
    }

    public static function fromAst(Node $node): static
    {
        $rangeSubselect = $node->getRangeSubselect();

        if ($rangeSubselect === null) {
            throw InvalidAstException::unexpectedNodeType('RangeSubselect', 'unknown');
        }

        $subqueryNode = $rangeSubselect->getSubquery();

        if ($subqueryNode === null) {
            throw InvalidAstException::missingRequiredField('subquery', 'RangeSubselect');
        }

        $selectStmt = $subqueryNode->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $valuesLists = $selectStmt->getValuesLists();

        if (\count($valuesLists) === 0) {
            throw InvalidAstException::invalidFieldValue('values_lists', 'SelectStmt', 'must have at least 1 row');
        }

        $rows = [];

        foreach ($valuesLists as $valuesList) {
            $list = $valuesList->getList();

            if ($list === null) {
                throw InvalidAstException::unexpectedNodeType('List', 'unknown');
            }

            $items = $list->getItems();
            $expressions = [];

            foreach ($items as $item) {
                $expressions[] = ExpressionFactory::fromAst($item);
            }

            $rows[] = new RowExpression($expressions);
        }

        return new self($rows);
    }

    public function as(string $alias, ?array $columnAliases = null): AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function toAst(): Node
    {
        $valuesLists = [];

        foreach ($this->rows as $row) {
            $valuesLists[] = $this->rowToListNode($row);
        }

        $selectStmt = new SelectStmt();
        $selectStmt->setOp(SetOperation::SETOP_NONE);
        $selectStmt->setValuesLists($valuesLists);

        $selectNode = new Node();
        $selectNode->setSelectStmt($selectStmt);

        $rangeSubselect = new RangeSubselect();
        $rangeSubselect->setSubquery($selectNode);
        $rangeSubselect->setLateral(false);

        $node = new Node();
        $node->setRangeSubselect($rangeSubselect);

        return $node;
    }

    private function rowToListNode(RowExpression $row): Node
    {
        $items = [];

        foreach ($row->args() as $arg) {
            $items[] = $arg->toAst();
        }

        $list = new PBList();
        $list->setItems($items);

        $node = new Node();
        $node->setList($list);

        return $node;
    }
}
