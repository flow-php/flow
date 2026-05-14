<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

use Flow\PostgreSql\Exception\ParserException;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;

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
    public function traverse(ParseResult $parseResult): void
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
    private function extractInnerNodes(Node $node): array
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

    private function traverseNode(Node $node): ?Node
    {
        if ($this->stopTraversal) {
            return null;
        }

        $traverseChildren = true;
        $replacement = null;

        $innerNodes = $this->extractInnerNodes($node);
        $parseResult = $this->parseResult;

        if ($parseResult === null) {
            throw new ParserException('traverseNode() called before traverse() initialized the parse result.');
        }

        $context = new ModificationContext($this->ancestorStack, $this->currentDepth, $parseResult);

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

    private function traverseNodeChildren(Node $node): void
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

            $whereClause = $selectStmt->getWhereClause();

            if ($whereClause !== null) {
                $this->traverseNode($whereClause);
            }

            $havingClause = $selectStmt->getHavingClause();

            if ($havingClause !== null) {
                $this->traverseNode($havingClause);
            }

            $limitOffset = $selectStmt->getLimitOffset();

            if ($limitOffset !== null) {
                $this->traverseNode($limitOffset);
            }

            $limitCount = $selectStmt->getLimitCount();

            if ($limitCount !== null) {
                $this->traverseNode($limitCount);
            }

            $largStmt = $selectStmt->getLarg();

            if ($largStmt !== null) {
                $larg = new Node();
                $larg->setSelectStmt($largStmt);
                $this->traverseNode($larg);
            }

            $rargStmt = $selectStmt->getRarg();

            if ($rargStmt !== null) {
                $rarg = new Node();
                $rarg->setSelectStmt($rargStmt);
                $this->traverseNode($rarg);
            }

            $withClause = $selectStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $insertStmt = $node->getInsertStmt();

        if ($insertStmt !== null) {
            $insertRelation = $insertStmt->getRelation();

            if ($insertRelation !== null) {
                $relationNode = new Node();
                $relationNode->setRangeVar($insertRelation);
                $this->traverseNode($relationNode);
            }
            $this->traverseRepeatedField($insertStmt->getCols());
            $this->traverseRepeatedField($insertStmt->getReturningList());

            $insertSelectStmt = $insertStmt->getSelectStmt();

            if ($insertSelectStmt !== null) {
                $this->traverseNode($insertSelectStmt);
            }

            $withClause = $insertStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $updateStmt = $node->getUpdateStmt();

        if ($updateStmt !== null) {
            $updateRelation = $updateStmt->getRelation();

            if ($updateRelation !== null) {
                $relationNode = new Node();
                $relationNode->setRangeVar($updateRelation);
                $this->traverseNode($relationNode);
            }
            $this->traverseRepeatedField($updateStmt->getTargetList());
            $this->traverseRepeatedField($updateStmt->getFromClause());
            $this->traverseRepeatedField($updateStmt->getReturningList());

            $updateWhereClause = $updateStmt->getWhereClause();

            if ($updateWhereClause !== null) {
                $this->traverseNode($updateWhereClause);
            }

            $withClause = $updateStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $deleteStmt = $node->getDeleteStmt();

        if ($deleteStmt !== null) {
            $deleteRelation = $deleteStmt->getRelation();

            if ($deleteRelation !== null) {
                $relationNode = new Node();
                $relationNode->setRangeVar($deleteRelation);
                $this->traverseNode($relationNode);
            }
            $this->traverseRepeatedField($deleteStmt->getUsingClause());
            $this->traverseRepeatedField($deleteStmt->getReturningList());

            $deleteWhereClause = $deleteStmt->getWhereClause();

            if ($deleteWhereClause !== null) {
                $this->traverseNode($deleteWhereClause);
            }

            $withClause = $deleteStmt->getWithClause();

            if ($withClause !== null) {
                $this->traverseRepeatedField($withClause->getCtes());
            }
        }

        $joinExpr = $node->getJoinExpr();

        if ($joinExpr !== null) {
            $joinLarg = $joinExpr->getLarg();

            if ($joinLarg !== null) {
                $this->traverseNode($joinLarg);
            }

            $joinRarg = $joinExpr->getRarg();

            if ($joinRarg !== null) {
                $this->traverseNode($joinRarg);
            }

            $joinQuals = $joinExpr->getQuals();

            if ($joinQuals !== null) {
                $this->traverseNode($joinQuals);
            }
            $this->traverseRepeatedField($joinExpr->getUsingClause());
        }

        $subLink = $node->getSubLink();

        if ($subLink !== null) {
            $subselect = $subLink->getSubselect();

            if ($subselect !== null) {
                $this->traverseNode($subselect);
            }
        }

        $rangeSubselect = $node->getRangeSubselect();

        if ($rangeSubselect !== null) {
            $subquery = $rangeSubselect->getSubquery();

            if ($subquery !== null) {
                $this->traverseNode($subquery);
            }
        }

        $cte = $node->getCommonTableExpr();

        if ($cte !== null) {
            $ctequery = $cte->getCtequery();

            if ($ctequery !== null) {
                $this->traverseNode($ctequery);
            }
        }

        $resTarget = $node->getResTarget();

        if ($resTarget !== null) {
            $resTargetVal = $resTarget->getVal();

            if ($resTargetVal !== null) {
                $this->traverseNode($resTargetVal);
            }
        }

        $funcCall = $node->getFuncCall();

        if ($funcCall !== null) {
            $this->traverseRepeatedField($funcCall->getArgs());
            $this->traverseRepeatedField($funcCall->getAggOrder());

            $aggFilter = $funcCall->getAggFilter();

            if ($aggFilter !== null) {
                $this->traverseNode($aggFilter);
            }
        }

        $aExpr = $node->getAExpr();

        if ($aExpr !== null) {
            $lexpr = $aExpr->getLexpr();

            if ($lexpr !== null) {
                $this->traverseNode($lexpr);
            }

            $rexpr = $aExpr->getRexpr();

            if ($rexpr !== null) {
                $this->traverseNode($rexpr);
            }
        }

        $boolExpr = $node->getBoolExpr();

        if ($boolExpr !== null) {
            $this->traverseRepeatedField($boolExpr->getArgs());
        }

        $caseExpr = $node->getCaseExpr();

        if ($caseExpr !== null) {
            $this->traverseRepeatedField($caseExpr->getArgs());

            $caseArg = $caseExpr->getArg();

            if ($caseArg !== null) {
                $this->traverseNode($caseArg);
            }

            $defresult = $caseExpr->getDefresult();

            if ($defresult !== null) {
                $this->traverseNode($defresult);
            }
        }

        $caseWhen = $node->getCaseWhen();

        if ($caseWhen !== null) {
            $caseWhenExpr = $caseWhen->getExpr();

            if ($caseWhenExpr !== null) {
                $this->traverseNode($caseWhenExpr);
            }

            $caseWhenResult = $caseWhen->getResult();

            if ($caseWhenResult !== null) {
                $this->traverseNode($caseWhenResult);
            }
        }

        $coalesceExpr = $node->getCoalesceExpr();

        if ($coalesceExpr !== null) {
            $this->traverseRepeatedField($coalesceExpr->getArgs());
        }

        $nullTest = $node->getNullTest();

        if ($nullTest !== null) {
            $nullTestArg = $nullTest->getArg();

            if ($nullTestArg !== null) {
                $this->traverseNode($nullTestArg);
            }
        }

        $typeCast = $node->getTypeCast();

        if ($typeCast !== null) {
            $typeCastArg = $typeCast->getArg();

            if ($typeCastArg !== null) {
                $this->traverseNode($typeCastArg);
            }
        }

        $sortBy = $node->getSortBy();

        if ($sortBy !== null) {
            $sortByNode = $sortBy->getNode();

            if ($sortByNode !== null) {
                $this->traverseNode($sortByNode);
            }
        }

        $rangeFunction = $node->getRangeFunction();

        if ($rangeFunction !== null) {
            $this->traverseRepeatedField($rangeFunction->getFunctions());
        }

        $booleanTest = $node->getBooleanTest();

        if ($booleanTest !== null) {
            $booleanTestArg = $booleanTest->getArg();

            if ($booleanTestArg !== null) {
                $this->traverseNode($booleanTestArg);
            }
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
            $indirectionArg = $indirection->getArg();

            if ($indirectionArg !== null) {
                $this->traverseNode($indirectionArg);
            }
            $this->traverseRepeatedField($indirection->getIndirection());
        }

        $minMaxExpr = $node->getMinMaxExpr();

        if ($minMaxExpr !== null) {
            $this->traverseRepeatedField($minMaxExpr->getArgs());
        }

        $namedArgExpr = $node->getNamedArgExpr();

        if ($namedArgExpr !== null) {
            $namedArgExprArg = $namedArgExpr->getArg();

            if ($namedArgExprArg !== null) {
                $this->traverseNode($namedArgExprArg);
            }
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
    private function traverseRepeatedField(?iterable $field): void
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
