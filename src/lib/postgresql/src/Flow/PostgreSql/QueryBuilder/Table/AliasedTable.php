<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents an aliased table reference: table AS alias or table AS alias (col1, col2).
 */
final readonly class AliasedTable implements TableReference
{
    /**
     * @param null|array<string> $columnAliases
     */
    public function __construct(
        public TableReference $table,
        public string $alias,
        public ?array $columnAliases = null,
    ) {}

    public static function fromAst(Node $node): static
    {
        $rangeVar = $node->getRangeVar();
        $joinExpr = $node->getJoinExpr();
        $rangeSubselect = $node->getRangeSubselect();
        $rangeFunction = $node->getRangeFunction();

        if ($rangeVar !== null) {
            return self::fromRangeVar($rangeVar);
        }

        if ($joinExpr !== null) {
            $joinAlias = $joinExpr->getAlias();

            if ($joinAlias === null) {
                throw InvalidAstException::missingRequiredField('alias', 'JoinExpr');
            }

            $joinedTable = JoinedTable::fromAst($node);

            return new self($joinedTable, $joinAlias->getAliasname(), self::extractColumnAliases($joinAlias));
        }

        if ($rangeSubselect !== null) {
            $alias = $rangeSubselect->getAlias();

            if ($alias === null) {
                throw InvalidAstException::missingRequiredField('alias', 'RangeSubselect');
            }

            $clonedRangeSubselect = clone $rangeSubselect;
            $clonedRangeSubselect->clearAlias();

            $nodeWithoutAlias = new Node();
            $nodeWithoutAlias->setRangeSubselect($clonedRangeSubselect);

            if ($rangeSubselect->getLateral()) {
                $table = Lateral::fromAst($node);
            } else {
                $table = SubqueryReference::fromAst($nodeWithoutAlias);
            }

            return new self($table, $alias->getAliasname(), self::extractColumnAliases($alias));
        }

        if ($rangeFunction !== null) {
            $alias = $rangeFunction->getAlias();

            if ($alias === null) {
                throw InvalidAstException::missingRequiredField('alias', 'RangeFunction');
            }

            $clonedRangeFunction = clone $rangeFunction;
            $clonedRangeFunction->clearAlias();

            $nodeWithoutAlias = new Node();
            $nodeWithoutAlias->setRangeFunction($clonedRangeFunction);

            if ($rangeFunction->getLateral()) {
                $table = Lateral::fromAst($node);
            } else {
                $table = TableFunction::fromAst($nodeWithoutAlias);
            }

            return new self($table, $alias->getAliasname(), self::extractColumnAliases($alias));
        }

        throw InvalidAstException::unexpectedNodeType(
            'RangeVar, JoinExpr, RangeSubselect, or RangeFunction',
            'unknown',
        );
    }

    public function as(string $alias, ?array $columnAliases = null): self
    {
        return new self($this->table, $alias, $columnAliases);
    }

    public function toAst(): Node
    {
        $tableNode = $this->table->toAst();

        if ($tableNode->hasRangeVar()) {
            $rangeVar = $tableNode->getRangeVar();

            if ($rangeVar === null) {
                throw InvalidAstException::missingRequiredField('range_var', 'Node');
            }

            $rangeVar->setAlias($this->createAlias());

            return new Node(['range_var' => $rangeVar]);
        }

        if ($tableNode->hasJoinExpr()) {
            $joinExpr = $tableNode->getJoinExpr();

            if ($joinExpr === null) {
                throw InvalidAstException::missingRequiredField('join_expr', 'Node');
            }

            $joinExpr->setAlias($this->createAlias());

            return new Node(['join_expr' => $joinExpr]);
        }

        if ($tableNode->hasRangeSubselect()) {
            $rangeSubselect = $tableNode->getRangeSubselect();

            if ($rangeSubselect === null) {
                throw InvalidAstException::missingRequiredField('range_subselect', 'Node');
            }

            $rangeSubselect->setAlias($this->createAlias());

            return new Node(['range_subselect' => $rangeSubselect]);
        }

        if ($tableNode->hasRangeFunction()) {
            $rangeFunction = $tableNode->getRangeFunction();

            if ($rangeFunction === null) {
                throw InvalidAstException::missingRequiredField('range_function', 'Node');
            }

            $rangeFunction->setAlias($this->createAlias());

            return new Node(['range_function' => $rangeFunction]);
        }

        throw InvalidAstException::unexpectedNodeType(
            'RangeVar, JoinExpr, RangeSubselect, or RangeFunction',
            'unknown',
        );
    }

    private function createAlias(): Alias
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

        return $alias;
    }

    /**
     * @return null|array<string>
     */
    private static function extractColumnAliases(Alias $alias): ?array
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

    private static function fromRangeVar(RangeVar $rangeVar): self
    {
        $alias = $rangeVar->getAlias();

        if ($alias === null) {
            throw InvalidAstException::missingRequiredField('alias', 'RangeVar');
        }

        $relname = $rangeVar->getRelname();

        if ($relname === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        $schemaname = $rangeVar->getSchemaname();
        $inh = $rangeVar->getInh();

        $table = new Table($relname, $schemaname !== '' ? $schemaname : null, $inh);

        return new self($table, $alias->getAliasname(), self::extractColumnAliases($alias));
    }
}
