<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Transformers;

use Flow\PgQuery\AST\{ModificationContext, NodeModifier};
use Flow\PgQuery\Exception\PaginationException;
use Flow\PgQuery\Protobuf\AST\{
    A_Const,
    A_Star,
    Alias,
    ColumnRef,
    Integer,
    LimitOption,
    Node,
    RangeSubselect,
    ResTarget,
    SelectStmt,
    SetOperation
};

/**
 * Modifies SELECT queries to add or modify LIMIT/OFFSET pagination.
 *
 * For set operations (UNION/INTERSECT/EXCEPT), the query is wrapped in a
 * subquery to ensure correct semantics:
 * SELECT * FROM (...original query...) AS _pagination_subq LIMIT x OFFSET y
 */
final readonly class PaginationModifier implements NodeModifier
{
    public function __construct(
        private PaginationConfig $config,
    ) {
    }

    public static function nodeClass() : string
    {
        return SelectStmt::class;
    }

    public function modify(object $node, ModificationContext $context) : int|object|null
    {
        /** @var SelectStmt $node */
        if (!$context->isTopLevel()) {
            return null;
        }

        if ($this->isSetOperation($node)) {
            return $this->wrapSetOperationWithPagination($node);
        }

        if ($this->hasExistingLimit($node)) {
            $this->handleExistingLimit($node);
        } else {
            $this->applyPagination($node);
        }

        return NodeModifier::DONT_TRAVERSE_CHILDREN;
    }

    private function applyPagination(SelectStmt $stmt) : void
    {
        $stmt->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
        $stmt->setLimitCount($this->createIntegerNode($this->config->limit));

        if ($this->config->offset > 0) {
            $stmt->setLimitOffset($this->createIntegerNode($this->config->offset));
        }
    }

    private function combineWithExisting(SelectStmt $stmt) : void
    {
        $existingLimit = $this->extractIntegerValue($stmt->getLimitCount());
        $existingOffset = $stmt->getLimitOffset() !== null
            ? $this->extractIntegerValue($stmt->getLimitOffset())
            : 0;

        $newLimit = \min($existingLimit, $this->config->limit);
        $newOffset = $existingOffset + $this->config->offset;

        $stmt->setLimitCount($this->createIntegerNode($newLimit));
        $stmt->setLimitOffset($this->createIntegerNode($newOffset));
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

    private function extractIntegerValue(?Node $node) : int
    {
        if ($node === null) {
            return 0;
        }

        $aConst = $node->getAConst();

        if ($aConst === null) {
            return 0;
        }

        /** @var null|int $ival (protobuf PHPDoc says int but actually returns Integer) */
        $ival = $aConst->getIval();

        return $ival ?? 0;
    }

    private function handleExistingLimit(SelectStmt $stmt) : void
    {
        match ($this->config->existingLimitBehavior) {
            ExistingLimitBehavior::OVERRIDE => $this->applyPagination($stmt),
            ExistingLimitBehavior::SKIP_IF_EXISTS => null,
            ExistingLimitBehavior::COMBINE_MINIMUM => $this->combineWithExisting($stmt),
            ExistingLimitBehavior::ERROR_IF_EXISTS => throw new PaginationException(
                'Query already contains LIMIT clause'
            ),
        };
    }

    private function hasExistingLimit(SelectStmt $stmt) : bool
    {
        return $stmt->getLimitCount() !== null;
    }

    private function isSetOperation(SelectStmt $stmt) : bool
    {
        $op = $stmt->getOp();

        return $op !== SetOperation::SETOP_NONE
            && $op !== SetOperation::SET_OPERATION_UNDEFINED;
    }

    private function wrapSetOperationWithPagination(SelectStmt $stmt) : Node
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
        $outerSelect->setLimitCount($this->createIntegerNode($this->config->limit));

        if ($this->config->offset > 0) {
            $outerSelect->setLimitOffset($this->createIntegerNode($this->config->offset));
        }

        $resultNode = new Node();
        $resultNode->setSelectStmt($outerSelect);

        return $resultNode;
    }
}
