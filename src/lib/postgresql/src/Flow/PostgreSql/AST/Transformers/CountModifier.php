<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\Protobuf\AST\A_Star;
use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;

/**
 * Transforms SELECT queries into COUNT queries for pagination.
 *
 * Wraps the original query in: SELECT COUNT(*) FROM (...) AS _count_subq
 * - Removes ORDER BY (not needed for counting)
 * - Removes LIMIT/OFFSET (we want total count)
 */
final readonly class CountModifier implements NodeModifier
{
    public static function nodeClasses(): array
    {
        return [SelectStmt::class];
    }

    public function modify(object $node, ModificationContext $context): int|object|null
    {
        /** @var SelectStmt $node */
        if (!$context->isTopLevel()) {
            return null;
        }

        $this->removeOrderBy($node);
        $this->removeLimitOffset($node);

        return $this->wrapWithCount($node);
    }

    private function createCountFunctionCall(): Node
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

    private function removeLimitOffset(SelectStmt $stmt): void
    {
        $stmt->clearLimitCount();
        $stmt->clearLimitOffset();
    }

    private function removeOrderBy(SelectStmt $stmt): void
    {
        $stmt->setSortClause([]);
    }

    private function wrapWithCount(SelectStmt $stmt): Node
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
