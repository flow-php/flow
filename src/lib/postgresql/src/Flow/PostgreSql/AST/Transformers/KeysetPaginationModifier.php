<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\ParamRefCollector;
use Flow\PostgreSql\Exception\PaginationException;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\LimitOption;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParamRef;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SortBy;
use Flow\PostgreSql\Protobuf\AST\SortByDir;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Table\SubqueryReference;

use function array_map;
use function count;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function iterator_to_array;
use function max;
use function sprintf;

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
    ) {}

    public static function nodeClasses(): array
    {
        return [ParseResult::class, SelectStmt::class];
    }

    public function modify(object $node, ModificationContext $context): int|object|null
    {
        if ($node instanceof ParseResult) {
            (new ParsedQuery($node))->statements()->assertReadOnlySelect();

            return null;
        }

        /** @var SelectStmt $node */
        if (!$context->isTopLevel()) {
            return null;
        }

        if (count($this->config->columns) === 0) {
            throw new PaginationException('Keyset pagination requires at least one column');
        }

        if ((new SelectStatement($node))->hasSetOperation()) {
            return $this->wrapSetOperationWithKeyset($node, $context);
        }

        if (!(new SelectStatement($node))->hasOrderBy()) {
            $this->addOrderByFromKeyset($node);
        }

        $this->applyLimit($node);
        $this->applyCursor($node, $context);

        return NodeModifier::DONT_TRAVERSE_CHILDREN;
    }

    /**
     * Adds ORDER BY clause to the statement based on keyset columns configuration.
     */
    private function addOrderByFromKeyset(SelectStmt $stmt): void
    {
        $sortNodes = [];

        foreach ($this->config->columns as $column) {
            $sortBy = new SortBy();
            $sortBy->setNode($this->createColumnRef($column->column));
            $sortBy->setSortbyDir($column->order === SortOrder::ASC ? SortByDir::SORTBY_ASC : SortByDir::SORTBY_DESC);

            $sortByNode = new Node();
            $sortByNode->setSortBy($sortBy);

            $sortNodes[] = $sortByNode;
        }

        $stmt->setSortClause($sortNodes);
    }

    private function applyCursor(SelectStmt $stmt, ModificationContext $context): void
    {
        $cursor = $this->config->cursor;

        if ($cursor instanceof Parameter) {
            $this->parameterOffset = $cursor->number() - 1;
            $this->applyKeysetWhere($stmt);
        } elseif ($cursor !== null) {
            if (count($cursor) !== count($this->config->columns)) {
                throw new PaginationException(sprintf(
                    'Cursor values count (%d) must match columns count (%d)',
                    count($cursor),
                    count($this->config->columns),
                ));
            }

            // the LIMIT parameter is counted explicitly: a wrapped set operation's outer select is not in the tree yet
            $this->parameterOffset = max(
                $this->detectMaxParamNumber($context),
                $this->config->limit instanceof Parameter ? $this->config->limit->number() : 0,
            );
            $this->applyKeysetWhere($stmt);
        }
    }

    private function applyKeysetWhere(SelectStmt $stmt): void
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

    private function applyLimit(SelectStmt $stmt): void
    {
        $stmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        $stmt->setLimitCount(
            ($this->config->limit instanceof Parameter ? $this->config->limit : literal($this->config->limit))->toAst(),
        );
    }

    private function buildComparisonExpr(Node $leftColumnRef, int $paramNumber, string $operator): Node
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

    private function buildKeysetCondition(): Node
    {
        $columns = $this->config->columns;
        $orConditions = [];

        for ($i = 0, $count = count($columns); $i < $count; $i++) {
            $andConditions = [];

            for ($j = 0; $j < $i; $j++) {
                $colRef = $this->createColumnRef($columns[$j]->column);
                $andConditions[] = $this->buildComparisonExpr($colRef, $j + 1, '=');
            }

            $colRef = $this->createColumnRef($columns[$i]->column);
            $operator = $columns[$i]->order === SortOrder::ASC ? '>' : '<';
            $andConditions[] = $this->buildComparisonExpr($colRef, $i + 1, $operator);

            if (count($andConditions) === 1) {
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

        if (count($orConditions) === 1) {
            return $orConditions[0];
        }

        $orExpr = new BoolExpr();
        $orExpr->setBoolop(BoolExprType::OR_EXPR);
        $orExpr->setArgs($orConditions);

        $orNode = new Node();
        $orNode->setBoolExpr($orExpr);

        return $orNode;
    }

    private function createColumnRef(string $columnName): Node
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

    private function detectMaxParamNumber(ModificationContext $context): int
    {
        $collector = new ParamRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($context->parseResult());

        return $collector->getMaxParamNumber();
    }

    private function wrapSetOperationWithKeyset(SelectStmt $stmt, ModificationContext $context): Node
    {
        foreach ($this->config->columns as $column) {
            if (QualifiedIdentifier::parse($column->column)->count() > 1) {
                throw new PaginationException(sprintf(
                    'Keyset column "%s" is qualified; a UNION/INTERSECT/EXCEPT result exposes only unqualified column names',
                    $column->column,
                ));
            }
        }

        // the set operation stays intact, including its own ORDER BY and LIMIT
        $outerSelect = select(star())
            ->from((new SubqueryReference((new Node())->setSelectStmt($stmt)))->as('_keyset_subq'))
            ->toAst();

        if ((new SelectStatement($stmt))->hasOrderBy()) {
            $outerSelect->setSortClause(array_map(static function (Node $sort): Node {
                $copy = new Node();
                $copy->mergeFrom($sort);

                return $copy;
            }, iterator_to_array($stmt->getSortClause())));
        } else {
            $this->addOrderByFromKeyset($outerSelect);
        }

        $this->applyLimit($outerSelect);
        $this->applyCursor($outerSelect, $context);

        return (new Node())->setSelectStmt($outerSelect);
    }
}
