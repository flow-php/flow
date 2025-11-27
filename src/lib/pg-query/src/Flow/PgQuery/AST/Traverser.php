<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST;

use Flow\PgQuery\Protobuf\AST\{Node, ParseResult};

/**
 * AST Traverser for PostgreSQL parse trees.
 *
 * Traverses the AST and calls registered visitors for specific node types.
 * Visitors only receive nodes of the type they are registered for.
 */
final class Traverser
{
    private bool $stopTraversal = false;

    /**
     * @var array<class-string, array<NodeVisitor>>
     */
    private readonly array $visitors;

    public function __construct(NodeVisitor ...$visitors)
    {
        $indexed = [];

        foreach ($visitors as $visitor) {
            $nodeClass = $visitor::nodeClass();
            $indexed[$nodeClass][] = $visitor;
        }

        $this->visitors = $indexed;
    }

    /**
     * Traverse a ParseResult.
     */
    public function traverse(ParseResult $parseResult) : void
    {
        $this->stopTraversal = false;

        foreach ($parseResult->getStmts() as $rawStmt) {
            $stmt = $rawStmt->getStmt();

            if ($stmt !== null && !$this->traverseNode($stmt)) {
                return;
            }
        }
    }

    /**
     * Extract inner node objects from the protobuf Node wrapper.
     *
     * @return array<object>
     */
    private function extractInnerNodes(Node $node) : array
    {
        $nodes = [];

        if (($inner = $node->getSelectStmt()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getInsertStmt()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getUpdateStmt()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getDeleteStmt()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getRangeVar()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getColumnRef()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getFuncCall()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getAExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getBoolExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getJoinExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getResTarget()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getSortBy()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getTypeCast()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getCoalesceExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getCaseExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getCaseWhen()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getNullTest()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getSubLink()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getCommonTableExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getRangeSubselect()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getRangeFunction()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getAlias()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getAConst()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getAStar()) !== null) {
            $nodes[] = $inner;
        }

        return $nodes;
    }

    private function traverseNode(Node $node) : bool
    {
        if ($this->stopTraversal) {
            return false;
        }

        $traverseChildren = true;

        $innerNodes = $this->extractInnerNodes($node);

        foreach ($innerNodes as $innerNode) {
            $nodeClass = $innerNode::class;

            if (isset($this->visitors[$nodeClass])) {
                foreach ($this->visitors[$nodeClass] as $visitor) {
                    $result = $visitor->enter($innerNode);

                    if ($result === NodeVisitor::STOP_TRAVERSAL) {
                        $this->stopTraversal = true;

                        return false;
                    }

                    if ($result === NodeVisitor::DONT_TRAVERSE_CHILDREN) {
                        $traverseChildren = false;
                    }
                }
            }
        }

        if ($traverseChildren) {
            $this->traverseNodeChildren($node);
        }

        foreach ($innerNodes as $innerNode) {
            $nodeClass = $innerNode::class;

            if (isset($this->visitors[$nodeClass])) {
                foreach ($this->visitors[$nodeClass] as $visitor) {
                    $result = $visitor->leave($innerNode);

                    if ($result === NodeVisitor::STOP_TRAVERSAL) {
                        $this->stopTraversal = true;

                        return false;
                    }
                }
            }
        }

        return true;
    }

    private function traverseNodeChildren(Node $node) : void
    {
        if ($this->stopTraversal) {
            return;
        }

        $selectStmt = $node->getSelectStmt();

        if ($selectStmt !== null) {
            $this->traverseRepeatedField($selectStmt->getFromClause());
            $this->traverseRepeatedField($selectStmt->getTargetList());
            $this->traverseRepeatedField($selectStmt->getGroupClause());
            $this->traverseRepeatedField($selectStmt->getSortClause());
            $this->traverseRepeatedField($selectStmt->getDistinctClause());
            $this->traverseRepeatedField($selectStmt->getWindowClause());
            $this->traverseRepeatedField($selectStmt->getValuesLists());
            $this->traverseRepeatedField($selectStmt->getLockingClause());

            if ($selectStmt->getWhereClause() !== null) {
                $this->traverseNode($selectStmt->getWhereClause());
            }

            if ($selectStmt->getHavingClause() !== null) {
                $this->traverseNode($selectStmt->getHavingClause());
            }

            if ($selectStmt->getLimitOffset() !== null) {
                $this->traverseNode($selectStmt->getLimitOffset());
            }

            if ($selectStmt->getLimitCount() !== null) {
                $this->traverseNode($selectStmt->getLimitCount());
            }

            if ($selectStmt->getLarg() !== null) {
                $larg = new Node();
                $larg->setSelectStmt($selectStmt->getLarg());
                $this->traverseNode($larg);
            }

            if ($selectStmt->getRarg() !== null) {
                $rarg = new Node();
                $rarg->setSelectStmt($selectStmt->getRarg());
                $this->traverseNode($rarg);
            }

            $withClause = $selectStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $insertStmt = $node->getInsertStmt();

        if ($insertStmt !== null) {
            if ($insertStmt->getRelation() !== null) {
                $relationNode = new Node();
                $relationNode->setRangeVar($insertStmt->getRelation());
                $this->traverseNode($relationNode);
            }
            $this->traverseRepeatedField($insertStmt->getCols());
            $this->traverseRepeatedField($insertStmt->getReturningList());

            if ($insertStmt->getSelectStmt() !== null) {
                $this->traverseNode($insertStmt->getSelectStmt());
            }

            $withClause = $insertStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $updateStmt = $node->getUpdateStmt();

        if ($updateStmt !== null) {
            if ($updateStmt->getRelation() !== null) {
                $relationNode = new Node();
                $relationNode->setRangeVar($updateStmt->getRelation());
                $this->traverseNode($relationNode);
            }
            $this->traverseRepeatedField($updateStmt->getTargetList());
            $this->traverseRepeatedField($updateStmt->getFromClause());
            $this->traverseRepeatedField($updateStmt->getReturningList());

            if ($updateStmt->getWhereClause() !== null) {
                $this->traverseNode($updateStmt->getWhereClause());
            }

            $withClause = $updateStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $deleteStmt = $node->getDeleteStmt();

        if ($deleteStmt !== null) {
            if ($deleteStmt->getRelation() !== null) {
                $relationNode = new Node();
                $relationNode->setRangeVar($deleteStmt->getRelation());
                $this->traverseNode($relationNode);
            }
            $this->traverseRepeatedField($deleteStmt->getUsingClause());
            $this->traverseRepeatedField($deleteStmt->getReturningList());

            if ($deleteStmt->getWhereClause() !== null) {
                $this->traverseNode($deleteStmt->getWhereClause());
            }

            $withClause = $deleteStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $joinExpr = $node->getJoinExpr();

        if ($joinExpr !== null) {
            if ($joinExpr->getLarg() !== null) {
                $this->traverseNode($joinExpr->getLarg());
            }

            if ($joinExpr->getRarg() !== null) {
                $this->traverseNode($joinExpr->getRarg());
            }

            if ($joinExpr->getQuals() !== null) {
                $this->traverseNode($joinExpr->getQuals());
            }
            $this->traverseRepeatedField($joinExpr->getUsingClause());
        }

        $subLink = $node->getSubLink();

        if ($subLink !== null && $subLink->getSubselect() !== null) {
            $this->traverseNode($subLink->getSubselect());
        }

        $rangeSubselect = $node->getRangeSubselect();

        if ($rangeSubselect !== null && $rangeSubselect->getSubquery() !== null) {
            $this->traverseNode($rangeSubselect->getSubquery());
        }

        $cte = $node->getCommonTableExpr();

        if ($cte !== null && $cte->getCtequery() !== null) {
            $this->traverseNode($cte->getCtequery());
        }

        $resTarget = $node->getResTarget();

        if ($resTarget !== null && $resTarget->getVal() !== null) {
            $this->traverseNode($resTarget->getVal());
        }

        $funcCall = $node->getFuncCall();

        if ($funcCall !== null) {
            $this->traverseRepeatedField($funcCall->getArgs());
            $this->traverseRepeatedField($funcCall->getAggOrder());

            if ($funcCall->getAggFilter() !== null) {
                $this->traverseNode($funcCall->getAggFilter());
            }
        }

        $aExpr = $node->getAExpr();

        if ($aExpr !== null) {
            if ($aExpr->getLexpr() !== null) {
                $this->traverseNode($aExpr->getLexpr());
            }

            if ($aExpr->getRexpr() !== null) {
                $this->traverseNode($aExpr->getRexpr());
            }
        }

        $boolExpr = $node->getBoolExpr();

        if ($boolExpr !== null) {
            $this->traverseRepeatedField($boolExpr->getArgs());
        }

        $caseExpr = $node->getCaseExpr();

        if ($caseExpr !== null) {
            $this->traverseRepeatedField($caseExpr->getArgs());

            if ($caseExpr->getArg() !== null) {
                $this->traverseNode($caseExpr->getArg());
            }

            if ($caseExpr->getDefresult() !== null) {
                $this->traverseNode($caseExpr->getDefresult());
            }
        }

        $caseWhen = $node->getCaseWhen();

        if ($caseWhen !== null) {
            if ($caseWhen->getExpr() !== null) {
                $this->traverseNode($caseWhen->getExpr());
            }

            if ($caseWhen->getResult() !== null) {
                $this->traverseNode($caseWhen->getResult());
            }
        }

        $coalesceExpr = $node->getCoalesceExpr();

        if ($coalesceExpr !== null) {
            $this->traverseRepeatedField($coalesceExpr->getArgs());
        }

        $nullTest = $node->getNullTest();

        if ($nullTest !== null && $nullTest->getArg() !== null) {
            $this->traverseNode($nullTest->getArg());
        }

        $typeCast = $node->getTypeCast();

        if ($typeCast !== null && $typeCast->getArg() !== null) {
            $this->traverseNode($typeCast->getArg());
        }

        $sortBy = $node->getSortBy();

        if ($sortBy !== null && $sortBy->getNode() !== null) {
            $this->traverseNode($sortBy->getNode());
        }

        $rangeFunction = $node->getRangeFunction();

        if ($rangeFunction !== null) {
            $this->traverseRepeatedField($rangeFunction->getFunctions());
        }
    }

    /**
     * @param null|iterable<Node> $field
     */
    private function traverseRepeatedField(?iterable $field) : void
    {
        if ($field === null) {
            return;
        }

        foreach ($field as $node) {
            if ($this->stopTraversal) {
                return;
            }

            $this->traverseNode($node);
        }
    }
}
