<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Transformers;

use Flow\PgQuery\AST\{ModificationContext, NodeModifier};
use Flow\PgQuery\Protobuf\AST\{
    Alias,
    ColumnRef,
    FuncCall,
    Node,
    PBString,
    RangeSubselect,
    ResTarget,
    SelectStmt
};
use Flow\PgQuery\Protobuf\AST\A_Star;

/**
 * Transforms SELECT queries into COUNT queries for pagination.
 *
 * Wraps the original query in: SELECT COUNT(*) FROM (...) AS _count_subq
 * - Removes ORDER BY (not needed for counting)
 * - Removes LIMIT/OFFSET (we want total count)
 */
final readonly class CountModifier implements NodeModifier
{
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

        $this->removeOrderBy($node);
        $this->removeLimitOffset($node);

        return $this->wrapWithCount($node);
    }

    private function createCountFunctionCall() : Node
    {
        $aStar = new A_Star();
        $aStarNode = new Node();
        $aStarNode->setAStar($aStar);

        $columnRef = new ColumnRef();
        $columnRef->setFields([$aStarNode]);

        $columnRefNode = new Node();
        $columnRefNode->setColumnRef($columnRef);

        $funcName = new PBString();
        $funcName->setSval('count');
        $funcNameNode = new Node();
        $funcNameNode->setString($funcName);

        $funcCall = new FuncCall();
        $funcCall->setFuncname([$funcNameNode]);
        $funcCall->setArgs([$columnRefNode]);
        $funcCall->setAggStar(true);

        $funcCallNode = new Node();
        $funcCallNode->setFuncCall($funcCall);

        return $funcCallNode;
    }

    private function removeLimitOffset(SelectStmt $stmt) : void
    {
        $stmt->clearLimitCount();
        $stmt->clearLimitOffset();
    }

    private function removeOrderBy(SelectStmt $stmt) : void
    {
        $stmt->setSortClause([]);
    }

    private function wrapWithCount(SelectStmt $stmt) : Node
    {
        $innerNode = new Node();
        $innerNode->setSelectStmt($stmt);

        $alias = new Alias();
        $alias->setAliasname('_count_subq');

        $rangeSubselect = new RangeSubselect();
        $rangeSubselect->setSubquery($innerNode);
        $rangeSubselect->setAlias($alias);

        $rangeSubselectNode = new Node();
        $rangeSubselectNode->setRangeSubselect($rangeSubselect);

        $resTarget = new ResTarget();
        $resTarget->setVal($this->createCountFunctionCall());

        $resTargetNode = new Node();
        $resTargetNode->setResTarget($resTarget);

        $outerSelect = new SelectStmt();
        $outerSelect->setTargetList([$resTargetNode]);
        $outerSelect->setFromClause([$rangeSubselectNode]);

        $resultNode = new Node();
        $resultNode->setSelectStmt($outerSelect);

        return $resultNode;
    }
}
