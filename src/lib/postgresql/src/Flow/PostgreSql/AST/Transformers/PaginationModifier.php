<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\Exception\PaginationException;
use Flow\PostgreSql\ParsedQuery;
use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\A_Star;
use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\LimitOption;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SetOperation;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;

use function count;

/**
 * Modifies SELECT queries to add LIMIT/OFFSET pagination.
 *
 * Always overrides any existing LIMIT/OFFSET clauses.
 * Validates that OFFSET requires ORDER BY (strict mode).
 *
 * For set operations (UNION/INTERSECT/EXCEPT), the query is wrapped in a
 * subquery to ensure correct semantics:
 * SELECT * FROM (...original query...) AS _pagination_subq LIMIT x OFFSET y
 */
final readonly class PaginationModifier implements NodeModifier
{
    public function __construct(
        private PaginationConfig $config,
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

        if ($this->hasOffset() && !$this->hasOrderBy($node)) {
            throw new PaginationException('OFFSET without ORDER BY produces non-deterministic results');
        }

        if ($this->isSetOperation($node)) {
            return $this->wrapSetOperationWithPagination($node);
        }

        $this->applyPagination($node);

        return NodeModifier::DONT_TRAVERSE_CHILDREN;
    }

    private function applyPagination(SelectStmt $stmt): void
    {
        $stmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        $stmt->setLimitCount($this->createValueNode($this->config->limit));

        if ($this->hasOffset()) {
            $stmt->setLimitOffset($this->createValueNode($this->config->offset));
        } else {
            $stmt->clearLimitOffset();
        }
    }

    private function createValueNode(int|Parameter $value): Node
    {
        if ($value instanceof Parameter) {
            return $value->toAst();
        }

        $integer = new Integer();
        $integer->setIval($value);

        $aConst = new A_Const(['ival' => $integer]);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function hasOffset(): bool
    {
        // a parameter's value is known only when the query runs, so it counts as an offset
        return $this->config->offset instanceof Parameter || $this->config->offset > 0;
    }

    private function hasOrderBy(SelectStmt $stmt): bool
    {
        return count($stmt->getSortClause()) > 0;
    }

    private function isSetOperation(SelectStmt $stmt): bool
    {
        $op = $stmt->getOp();

        return $op !== SetOperation::SETOP_NONE && $op !== SetOperation::SET_OPERATION_UNDEFINED;
    }

    private function wrapSetOperationWithPagination(SelectStmt $stmt): Node
    {
        $innerNode = new Node();
        $innerNode->setSelectStmt($stmt);

        $alias = new Alias();
        $alias->setAliasname('_pagination_subq');

        $rangeSubselect = new RangeSubselect();
        $rangeSubselect->setSubquery($innerNode);
        $rangeSubselect->setAlias($alias);

        $rangeSubselectNode = new Node();
        $rangeSubselectNode->setRangeSubselect($rangeSubselect);

        $aStar = new A_Star();
        $aStarNode = new Node();
        $aStarNode->setAStar($aStar);

        $columnRef = new ColumnRef();
        $columnRef->setFields([$aStarNode]);

        $columnRefNode = new Node();
        $columnRefNode->setColumnRef($columnRef);

        $resTarget = new ResTarget();
        $resTarget->setVal($columnRefNode);

        $resTargetNode = new Node();
        $resTargetNode->setResTarget($resTarget);

        $outerSelect = new SelectStmt();
        $outerSelect->setTargetList([$resTargetNode]);
        $outerSelect->setFromClause([$rangeSubselectNode]);
        $outerSelect->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        $outerSelect->setLimitCount($this->createValueNode($this->config->limit));

        if ($this->hasOffset()) {
            $outerSelect->setLimitOffset($this->createValueNode($this->config->offset));
        }

        $resultNode = new Node();
        $resultNode->setSelectStmt($outerSelect);

        return $resultNode;
    }
}
