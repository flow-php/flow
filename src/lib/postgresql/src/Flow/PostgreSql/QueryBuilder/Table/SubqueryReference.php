<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a subquery as a table reference: (SELECT ...).
 * Note: Subqueries in FROM clause must be aliased in actual SQL.
 */
final readonly class SubqueryReference implements TableReference
{
    /**
     * @param Node $subquery The SELECT statement node
     */
    public function __construct(
        private Node $subquery,
    ) {}

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

        return new self($subqueryNode);
    }

    public function as(string $alias, ?array $columnAliases = null): AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function getSubquery(): Node
    {
        return $this->subquery;
    }

    public function toAst(): Node
    {
        $rangeSubselect = new RangeSubselect();
        $rangeSubselect->setSubquery($this->subquery);
        $rangeSubselect->setLateral(false);

        $node = new Node();
        $node->setRangeSubselect($rangeSubselect);

        return $node;
    }
}
