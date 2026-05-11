<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\JoinExpr;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * Represents a joined table: table1 JOIN table2 ON condition or table1 JOIN table2 USING (col1, col2).
 */
final readonly class JoinedTable implements TableReference
{
    /**
     * @param null|array<string> $usingColumns
     */
    public function __construct(
        public TableReference $left,
        public TableReference $right,
        public JoinType $joinType,
        public ?Condition $onCondition = null,
        public ?array $usingColumns = null,
        public bool $natural = false,
    ) {}

    public static function fromAst(Node $node): static
    {
        $joinExpr = $node->getJoinExpr();

        if ($joinExpr === null) {
            throw InvalidAstException::unexpectedNodeType('JoinExpr', 'unknown');
        }

        $larg = $joinExpr->getLarg();

        if ($larg === null) {
            throw InvalidAstException::missingRequiredField('larg', 'JoinExpr');
        }

        $rarg = $joinExpr->getRarg();

        if ($rarg === null) {
            throw InvalidAstException::missingRequiredField('rarg', 'JoinExpr');
        }

        $left = self::tableReferenceFromNode($larg);
        $right = self::tableReferenceFromNode($rarg);

        $joinType = JoinType::fromProtobuf($joinExpr->getJointype());
        $natural = $joinExpr->getIsNatural();

        $onCondition = null;
        $quals = $joinExpr->getQuals();

        if ($quals !== null) {
            $onCondition = ConditionFactory::fromAst($quals);
        }

        $usingColumns = null;
        $usingClause = $joinExpr->getUsingClause();

        if (\count($usingClause) > 0) {
            $usingColumns = [];

            foreach ($usingClause as $col) {
                $string = $col->getString();

                if ($string !== null) {
                    $usingColumns[] = $string->getSval();
                }
            }

            if (\count($usingColumns) === 0) {
                $usingColumns = null;
            }
        }

        return new self($left, $right, $joinType, $onCondition, $usingColumns, $natural);
    }

    public function as(string $alias, ?array $columnAliases = null): AliasedTable
    {
        return new AliasedTable($this, $alias, $columnAliases);
    }

    public function toAst(): Node
    {
        $joinExpr = new JoinExpr([
            'jointype' => $this->joinType->toProtobuf(),
            'larg' => $this->left->toAst(),
            'rarg' => $this->right->toAst(),
            'is_natural' => $this->natural,
        ]);

        if ($this->onCondition !== null) {
            $joinExpr->setQuals($this->onCondition->toAst());
        }

        if ($this->usingColumns !== null) {
            $usingClause = [];

            foreach ($this->usingColumns as $col) {
                $usingClause[] = new Node(['string' => new PBString(['sval' => $col])]);
            }

            $joinExpr->setUsingClause($usingClause);
        }

        return new Node(['join_expr' => $joinExpr]);
    }

    private static function tableReferenceFromNode(Node $node): TableReference
    {
        if ($node->hasRangeVar()) {
            $rangeVar = $node->getRangeVar();

            if ($rangeVar === null) {
                throw InvalidAstException::missingRequiredField('range_var', 'Node');
            }

            $alias = $rangeVar->getAlias();

            if ($alias !== null) {
                return AliasedTable::fromAst($node);
            }

            return Table::fromAst($node);
        }

        if ($node->hasJoinExpr()) {
            $joinExpr = $node->getJoinExpr();

            if ($joinExpr === null) {
                throw InvalidAstException::missingRequiredField('join_expr', 'Node');
            }

            $alias = $joinExpr->getAlias();

            if ($alias !== null) {
                return AliasedTable::fromAst($node);
            }

            return self::fromAst($node);
        }

        if ($node->hasRangeSubselect()) {
            return DerivedTable::fromAst($node);
        }

        throw InvalidAstException::unexpectedNodeType('RangeVar, JoinExpr or RangeSubselect', 'unknown');
    }
}
