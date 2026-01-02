<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\{ModificationContext, NodeModifier, Traverser};
use Flow\PostgreSql\AST\Visitors\ParamRefCollector;
use Flow\PostgreSql\Exception\PaginationException;
use Flow\PostgreSql\Protobuf\AST\{
    A_Const,
    A_Expr,
    A_Expr_Kind,
    BoolExpr,
    BoolExprType,
    ColumnRef,
    Integer,
    LimitOption,
    Node,
    PBString,
    ParamRef,
    SelectStmt,
    SortBy,
    SortByDir
};
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;

/**
 * Applies keyset (cursor-based) pagination to SELECT queries.
 *
 * Keyset pagination is more efficient than OFFSET for large datasets because
 * it uses indexed WHERE conditions instead of skipping rows.
 *
 * For first page (no cursor): Just adds LIMIT
 * For subsequent pages: Adds WHERE (col1, col2) > ($1, $2) AND existing_where
 *
 * Automatically detects existing query parameters and appends keyset placeholders
 * at the end (e.g., if query has $1, $2, keyset uses $3, $4, etc.)
 */
final class KeysetPaginationModifier implements NodeModifier
{
    private int $parameterOffset = 0;

    public function __construct(
        private readonly KeysetPaginationConfig $config,
    ) {
    }

    public static function nodeClass() : string
    {
        return SelectStmt::class;
    }

    /** @phpstan-ignore return.unusedType (interface requires full signature) */
    public function modify(object $node, ModificationContext $context) : int|object|null
    {
        /** @var SelectStmt $node */
        if (!$context->isTopLevel()) {
            return null;
        }

        if (\count($this->config->columns) === 0) {
            throw new PaginationException('Keyset pagination requires at least one column');
        }

        $this->parameterOffset = $this->detectMaxParamNumber($context);

        if (!$this->hasOrderBy($node)) {
            $this->addOrderByFromKeyset($node);
        }

        $this->applyLimit($node);

        if ($this->config->cursor !== null) {
            if (\count($this->config->cursor) !== \count($this->config->columns)) {
                throw new PaginationException(\sprintf(
                    'Cursor values count (%d) must match columns count (%d)',
                    \count($this->config->cursor),
                    \count($this->config->columns)
                ));
            }

            $this->applyKeysetWhere($node);
        }

        return NodeModifier::DONT_TRAVERSE_CHILDREN;
    }

    /**
     * Adds ORDER BY clause to the statement based on keyset columns configuration.
     */
    private function addOrderByFromKeyset(SelectStmt $stmt) : void
    {
        $sortNodes = [];

        foreach ($this->config->columns as $column) {
            $sortBy = new SortBy();
            $sortBy->setNode($this->createColumnRef($column->column));
            $sortBy->setSortbyDir(
                $column->order === SortOrder::ASC ? SortByDir::SORTBY_ASC : SortByDir::SORTBY_DESC
            );

            $sortByNode = new Node();
            $sortByNode->setSortBy($sortBy);

            $sortNodes[] = $sortByNode;
        }

        $stmt->setSortClause($sortNodes);
    }

    private function applyKeysetWhere(SelectStmt $stmt) : void
    {
        $keysetCondition = $this->buildKeysetCondition();

        $existingWhere = $stmt->getWhereClause();

        if ($existingWhere !== null) {
            $andExpr = new BoolExpr();
            $andExpr->setBoolop(BoolExprType::AND_EXPR);
            $andExpr->setArgs([$existingWhere, $keysetCondition]);

            $andNode = new Node();
            $andNode->setBoolExpr($andExpr);

            $stmt->setWhereClause($andNode);
        } else {
            $stmt->setWhereClause($keysetCondition);
        }
    }

    private function applyLimit(SelectStmt $stmt) : void
    {
        $stmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        $stmt->setLimitCount($this->createIntegerNode($this->config->limit));
    }

    private function buildComparisonExpr(Node $leftColumnRef, int $paramNumber, string $operator) : Node
    {
        $paramRef = new ParamRef();
        $paramRef->setNumber($paramNumber + $this->parameterOffset);

        $paramNode = new Node();
        $paramNode->setParamRef($paramRef);

        $opName = new PBString();
        $opName->setSval($operator);
        $opNameNode = new Node();
        $opNameNode->setString($opName);

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);
        $aExpr->setName([$opNameNode]);
        $aExpr->setLexpr($leftColumnRef);
        $aExpr->setRexpr($paramNode);

        $exprNode = new Node();
        $exprNode->setAExpr($aExpr);

        return $exprNode;
    }

    private function buildKeysetCondition() : Node
    {
        $columns = $this->config->columns;
        $orConditions = [];

        for ($i = 0, $count = \count($columns); $i < $count; $i++) {
            $andConditions = [];

            for ($j = 0; $j < $i; $j++) {
                $colRef = $this->createColumnRef($columns[$j]->column);
                $andConditions[] = $this->buildComparisonExpr($colRef, $j + 1, '=');
            }

            $colRef = $this->createColumnRef($columns[$i]->column);
            $operator = $columns[$i]->order === SortOrder::ASC ? '>' : '<';
            $andConditions[] = $this->buildComparisonExpr($colRef, $i + 1, $operator);

            if (\count($andConditions) === 1) {
                $orConditions[] = $andConditions[0];
            } else {
                $andExpr = new BoolExpr();
                $andExpr->setBoolop(BoolExprType::AND_EXPR);
                $andExpr->setArgs($andConditions);

                $andNode = new Node();
                $andNode->setBoolExpr($andExpr);

                $orConditions[] = $andNode;
            }
        }

        if (\count($orConditions) === 1) {
            return $orConditions[0];
        }

        $orExpr = new BoolExpr();
        $orExpr->setBoolop(BoolExprType::OR_EXPR);
        $orExpr->setArgs($orConditions);

        $orNode = new Node();
        $orNode->setBoolExpr($orExpr);

        return $orNode;
    }

    private function createColumnRef(string $columnName) : Node
    {
        $fields = [];

        foreach (QualifiedIdentifier::parse($columnName)->parts() as $part) {
            $str = new PBString();
            $str->setSval($part);
            $strNode = new Node();
            $strNode->setString($str);
            $fields[] = $strNode;
        }

        $columnRef = new ColumnRef();
        $columnRef->setFields($fields);

        $columnRefNode = new Node();
        $columnRefNode->setColumnRef($columnRef);

        return $columnRefNode;
    }

    private function createIntegerNode(int $value) : Node
    {
        $integer = new Integer();
        $integer->setIval($value);

        $aConst = new A_Const();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($integer);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function detectMaxParamNumber(ModificationContext $context) : int
    {
        $collector = new ParamRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($context->parseResult());

        return $collector->getMaxParamNumber();
    }

    private function hasOrderBy(SelectStmt $stmt) : bool
    {
        return \count($stmt->getSortClause() ?? []) > 0;
    }
}
