<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Table;

use Flow\PgQuery\Protobuf\AST\{Node, RangeVar};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, InvalidTableException};

/**
 * Represents a reference to a Common Table Expression (CTE) by name.
 * Used in WITH clause references: cte_name.
 */
final readonly class CTEReference implements TableReference
{
    /**
     * @param non-empty-string $cteName
     */
    public function __construct(
        private string $cteName,
    ) {
        if ($this->cteName === '') {
            throw InvalidTableException::emptyArray('CTE name');
        }
    }

    public static function fromAst(Node $node) : static
    {
        $rangeVar = $node->getRangeVar();

        if ($rangeVar === null) {
            throw InvalidAstException::unexpectedNodeType('RangeVar', 'unknown');
        }

        $relname = $rangeVar->getRelname();

        if ($relname === null || $relname === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        return new self($relname);
    }

    public function as(string $alias, ?array $columnAliases = null) : AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    /**
     * @return non-empty-string
     */
    public function getCteName() : string
    {
        return $this->cteName;
    }

    public function toAst() : Node
    {
        $rangeVar = new RangeVar();
        $rangeVar->setRelname($this->cteName);
        $rangeVar->setInh(true);

        $node = new Node();
        $node->setRangeVar($rangeVar);

        return $node;
    }
}
