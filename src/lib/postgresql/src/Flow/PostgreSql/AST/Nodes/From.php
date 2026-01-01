<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidFromNodeException;
use Flow\PostgreSql\Protobuf\AST\Node;

final readonly class From implements \Countable
{
    /**
     * @var array<Node>
     */
    private array $nodes;

    /**
     * @param array<Node> $nodes
     */
    public function __construct(array $nodes)
    {
        foreach ($nodes as $node) {
            if (!$this->isValidFromNode($node)) {
                throw InvalidFromNodeException::invalidNode($node);
            }
        }
        $this->nodes = $nodes;
    }

    public function count() : int
    {
        return \count($this->nodes);
    }

    public function hasFunction() : bool
    {
        foreach ($this->nodes as $fromNode) {
            if ($fromNode->getRangeFunction() !== null) {
                return true;
            }
        }

        return false;
    }

    public function hasValues() : bool
    {
        foreach ($this->nodes as $fromNode) {
            $rangeSubselect = $fromNode->getRangeSubselect();

            if ($rangeSubselect === null) {
                continue;
            }

            $subquery = $rangeSubselect->getSubquery();

            if ($subquery === null) {
                continue;
            }

            $selectStmt = $subquery->getSelectStmt();

            if ($selectStmt !== null && \count($selectStmt->getValuesLists()) > 0) {
                return true;
            }
        }

        return false;
    }

    public function isEmpty() : bool
    {
        return $this->count() === 0;
    }

    public function tables() : Tables
    {
        $tables = [];

        foreach ($this->nodes as $node) {
            $rangeVar = $node->getRangeVar();

            if ($rangeVar !== null) {
                $tables[] = new Table($rangeVar);
            }
        }

        return new Tables($tables);
    }

    private function isValidFromNode(Node $node) : bool
    {
        return $node->getRangeVar() !== null
            || $node->getRangeSubselect() !== null
            || $node->getJoinExpr() !== null
            || $node->getRangeFunction() !== null
            || $node->getRangeTableFunc() !== null
            || $node->getRangeTableSample() !== null
            || $node->getJsonTable() !== null;
    }
}
