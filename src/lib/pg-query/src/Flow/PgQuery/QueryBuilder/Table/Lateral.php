<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Table;

use Flow\PgQuery\Protobuf\AST\{Node, RangeSubselect};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, InvalidTableException};

/**
 * Represents a LATERAL subquery or function reference.
 * LATERAL allows a subquery or function to reference columns from preceding tables.
 */
final readonly class Lateral implements TableReference
{
    /**
     * @param TableReference $reference Either a subquery reference (RangeSubselect) or a table function
     */
    public function __construct(
        private TableReference $reference,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $rangeSubselect = $node->getRangeSubselect();
        $rangeFunction = $node->getRangeFunction();

        if ($rangeSubselect !== null) {
            if (!$rangeSubselect->getLateral()) {
                throw InvalidAstException::invalidFieldValue('lateral', 'RangeSubselect', 'expected true for LATERAL');
            }

            $subqueryNode = $rangeSubselect->getSubquery();

            if ($subqueryNode === null) {
                throw InvalidAstException::missingRequiredField('subquery', 'RangeSubselect');
            }

            return new self(new SubqueryReference($subqueryNode));
        }

        if ($rangeFunction !== null) {
            if (!$rangeFunction->getLateral()) {
                throw InvalidAstException::invalidFieldValue('lateral', 'RangeFunction', 'expected true for LATERAL');
            }

            $rangeFunctionWithoutLateral = clone $rangeFunction;
            $rangeFunctionWithoutLateral->setLateral(false);

            $functionNode = new Node();
            $functionNode->setRangeFunction($rangeFunctionWithoutLateral);

            return new self(TableFunction::fromAst($functionNode));
        }

        throw InvalidAstException::unexpectedNodeType('RangeSubselect or RangeFunction with lateral=true', 'unknown');
    }

    public function as(string $alias, ?array $columnAliases = null) : AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function getReference() : TableReference
    {
        return $this->reference;
    }

    public function toAst() : Node
    {
        $referenceNode = $this->reference->toAst();

        $rangeSubselect = $referenceNode->getRangeSubselect();

        if ($rangeSubselect !== null) {
            $rangeSubselect->setLateral(true);

            return $referenceNode;
        }

        $rangeFunction = $referenceNode->getRangeFunction();

        if ($rangeFunction !== null) {
            $rangeFunction->setLateral(true);

            return $referenceNode;
        }

        throw InvalidTableException::invalidType('SubqueryReference or TableFunction', \get_debug_type($this->reference));
    }
}
