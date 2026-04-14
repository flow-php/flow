<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

use Flow\PostgreSql\Protobuf\AST\{Node, ParseResult};

/**
 * AST Traverser for PostgreSQL parse trees.
 *
 * Traverses the AST and calls registered visitors and modifiers for specific node types.
 * Visitors receive nodes for read-only operations (collection, analysis).
 * Modifiers receive nodes with context for mutation operations.
 */
final class Traverser
{
    /** @var array<object> */
    private array $ancestorStack = [];

    private int $currentDepth = 0;

    /**
     * @var array<class-string, array<NodeModifier>>
     */
    private readonly array $modifiers;

    private ?ParseResult $parseResult = null;

    private bool $stopTraversal = false;

    /**
     * @var array<class-string, array<NodeVisitor>>
     */
    private readonly array $visitors;

    public function __construct(NodeVisitor|NodeModifier ...$handlers)
    {
        $visitors = [];
        $modifiers = [];

        foreach ($handlers as $handler) {
            foreach ($handler::nodeClasses() as $nodeClass) {
                if ($handler instanceof NodeModifier) {
                    $modifiers[$nodeClass][] = $handler;
                }

                if ($handler instanceof NodeVisitor) {
                    $visitors[$nodeClass][] = $handler;
                }
            }
        }

        $this->visitors = $visitors;
        $this->modifiers = $modifiers;
    }

    /**
     * Traverse a ParseResult.
     */
    public function traverse(ParseResult $parseResult) : void
    {
        $this->stopTraversal = false;
        $this->ancestorStack = [];
        $this->currentDepth = 0;
        $this->parseResult = $parseResult;

        foreach ($parseResult->getStmts() as $rawStmt) {
            $this->currentDepth = 1;
            $stmt = $rawStmt->getStmt();

            if ($stmt !== null) {
                $replacement = $this->traverseNode($stmt);

                if ($replacement instanceof Node) {
                    $rawStmt->setStmt($replacement);
                }

                /** @phpstan-ignore if.alwaysFalse (stopTraversal can be modified by traverseNode) */
                if ($this->stopTraversal) {
                    return;
                }
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

        if (($inner = $node->getParamRef()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getBooleanTest()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getRowExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getAArrayExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getAIndirection()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getMinMaxExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getNamedArgExpr()) !== null) {
            $nodes[] = $inner;
        }

        if (($inner = $node->getXmlExpr()) !== null) {
            $nodes[] = $inner;
        }

        return $nodes;
    }

    private function traverseNode(Node $node) : ?Node
    {
        if ($this->stopTraversal) {
            return null;
        }

        $traverseChildren = true;
        $replacement = null;

        $innerNodes = $this->extractInnerNodes($node);
        /** @phpstan-ignore argument.type (parseResult is always set by traverse() before traverseNode() is called) */
        $context = new ModificationContext($this->ancestorStack, $this->currentDepth, $this->parseResult);

        foreach ($innerNodes as $innerNode) {
            $nodeClass = $innerNode::class;

            if (isset($this->modifiers[$nodeClass])) {
                foreach ($this->modifiers[$nodeClass] as $modifier) {
                    $result = $modifier->modify($innerNode, $context);

                    if ($result === NodeModifier::STOP_TRAVERSAL) {
                        $this->stopTraversal = true;

                        return null;
                    }

                    if ($result === NodeModifier::DONT_TRAVERSE_CHILDREN) {
                        $traverseChildren = false;
                    }

                    if ($result instanceof Node) {
                        $replacement = $result;
                        $traverseChildren = false;
                    }
                }
            }

            if (isset($this->visitors[$nodeClass])) {
                foreach ($this->visitors[$nodeClass] as $visitor) {
                    $result = $visitor->enter($innerNode);

                    if ($result === NodeVisitor::STOP_TRAVERSAL) {
                        $this->stopTraversal = true;

                        return null;
                    }

                    if ($result === NodeVisitor::DONT_TRAVERSE_CHILDREN) {
                        $traverseChildren = false;
                    }
                }
            }
        }

        if ($traverseChildren) {
            $this->ancestorStack[] = $node;
            $this->currentDepth++;
            $this->traverseNodeChildren($node);
            \array_pop($this->ancestorStack);
            $this->currentDepth--;
        }

        foreach ($innerNodes as $innerNode) {
            $nodeClass = $innerNode::class;

            if (isset($this->visitors[$nodeClass])) {
                foreach ($this->visitors[$nodeClass] as $visitor) {
                    $result = $visitor->leave($innerNode);

                    if ($result === NodeVisitor::STOP_TRAVERSAL) {
                        $this->stopTraversal = true;

                        return null;
                    }
                }
            }
        }

        return $replacement;
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

        $booleanTest = $node->getBooleanTest();

        if ($booleanTest !== null && $booleanTest->getArg() !== null) {
            $this->traverseNode($booleanTest->getArg());
        }

        $rowExpr = $node->getRowExpr();

        if ($rowExpr !== null) {
            $this->traverseRepeatedField($rowExpr->getArgs());
        }

        $arrayExpr = $node->getAArrayExpr();

        if ($arrayExpr !== null) {
            $this->traverseRepeatedField($arrayExpr->getElements());
        }

        $indirection = $node->getAIndirection();

        if ($indirection !== null) {
            if ($indirection->getArg() !== null) {
                $this->traverseNode($indirection->getArg());
            }
            $this->traverseRepeatedField($indirection->getIndirection());
        }

        $minMaxExpr = $node->getMinMaxExpr();

        if ($minMaxExpr !== null) {
            $this->traverseRepeatedField($minMaxExpr->getArgs());
        }

        $namedArgExpr = $node->getNamedArgExpr();

        if ($namedArgExpr !== null && $namedArgExpr->getArg() !== null) {
            $this->traverseNode($namedArgExpr->getArg());
        }

        $xmlExpr = $node->getXmlExpr();

        if ($xmlExpr !== null) {
            $this->traverseRepeatedField($xmlExpr->getArgs());
            $this->traverseRepeatedField($xmlExpr->getNamedArgs());
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
