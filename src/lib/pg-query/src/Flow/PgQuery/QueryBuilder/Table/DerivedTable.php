<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Table;

use Flow\PgQuery\Protobuf\AST\{Alias, Node, PBString, RangeSubselect};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a derived table (subquery): (SELECT ...) AS alias or LATERAL (SELECT ...) AS alias (col1, col2).
 */
final readonly class DerivedTable implements TableReference
{
    /**
     * @param null|array<string> $columnAliases
     */
    public function __construct(
        public Node $subquery,
        public string $alias,
        public ?array $columnAliases = null,
        public bool $lateral = false,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $rangeSubselect = $node->getRangeSubselect();

        if ($rangeSubselect === null) {
            throw InvalidAstException::unexpectedNodeType('RangeSubselect', 'unknown');
        }

        $subquery = $rangeSubselect->getSubquery();

        if ($subquery === null) {
            throw InvalidAstException::missingRequiredField('subquery', 'RangeSubselect');
        }

        $alias = $rangeSubselect->getAlias();

        if ($alias === null) {
            throw InvalidAstException::missingRequiredField('alias', 'RangeSubselect');
        }

        $lateral = $rangeSubselect->getLateral();

        return new self(
            $subquery,
            $alias->getAliasname(),
            self::extractColumnAliases($alias),
            $lateral
        );
    }

    public function as(string $alias, ?array $columnAliases = null) : AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function toAst() : Node
    {
        $alias = new Alias([
            'aliasname' => $this->alias,
        ]);

        if ($this->columnAliases !== null) {
            $colnames = [];

            foreach ($this->columnAliases as $colname) {
                $colnames[] = new Node(['string' => new PBString(['sval' => $colname])]);
            }

            $alias->setColnames($colnames);
        }

        $rangeSubselect = new RangeSubselect([
            'subquery' => $this->subquery,
            'alias' => $alias,
            'lateral' => $this->lateral,
        ]);

        return new Node(['range_subselect' => $rangeSubselect]);
    }

    /**
     * @return null|array<string>
     */
    private static function extractColumnAliases(Alias $alias) : ?array
    {
        $colnames = $alias->getColnames();

        if (\count($colnames) === 0) {
            return null;
        }

        $columnAliases = [];

        foreach ($colnames as $colname) {
            $string = $colname->getString();

            if ($string === null) {
                continue;
            }

            $columnAliases[] = $string->getSval();
        }

        return \count($columnAliases) > 0 ? $columnAliases : null;
    }
}
