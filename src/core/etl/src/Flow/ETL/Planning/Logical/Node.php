<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

interface Node
{
    /**
     * Inputs, in the order they are planned. children()[0] is the node's ROW INPUT - the chain a rule walks down and
     * the chain Planner::pipelines() cuts into pipelines. A join's second child is the joined frame's whole plan: its
     * Result or Outputs root, read without taking its rows as input and never rewritten by this plan's rewrites.
     * A leaf returns [].
     *
     * Two kinds break that shape. A Transaction is a write root whose children are sibling write roots it commits
     * together - never a row input, never on a spine. Outputs is the root of a plan with several consumers:
     * children()[0] is the spine consumer, and every further child is a consumer too.
     *
     * A plan's root is whatever consumer its Trigger built - a Result, a write, or an Outputs over several. A Result
     * or Outputs is also the first node of a frame that read another frame, and the right child of a join.
     *
     * @return list<Node>
     */
    public function children(): array;

    /**
     * The same node over new children. Returns $this when the children are the ones it already holds, so
     * an untouched subtree keeps its identity and its planned steps (PlannedNodes remembers them by identity).
     *
     * @param list<Node> $children exactly as many as children() returns
     */
    public function withChildren(array $children): self;

    /**
     * What this node does to the row count. Answered from what the node holds, never from a class list.
     */
    public function rowCount(): RowCount;

    /**
     * Whether the node's output is a function of the rows it is handed, one at a time, with no effect
     * outside the stream.
     */
    public function transparency(): Transparency;

    /**
     * Columns this node introduces, renames or removes on its output. A predicate that references one of them means a
     * different column below this node, so it cannot be pushed past it. Answered from what the node holds,
     * never from a class list.
     */
    public function redefines(): Redefined;
}
