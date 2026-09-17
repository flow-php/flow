<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

interface Node
{
    /**
     * Inputs, in the order they are planned. children()[0] is the node's ROW INPUT - the chain a rule walks down and
     * the chain PipelineSplit turns into segments. Every further child is a SIDE INPUT and is always a
     * SideInput: another plan this node reads without taking its rows as input. A leaf returns [].
     *
     * Three kinds break that shape. A SideInput is a LEAF that owns a whole plan - read it through plan(),
     * a rule over the outer plan never descends into it. A Transaction is a ROOT-ONLY node whose children
     * are sibling sink roots it commits together - never a row input, never on a spine. Outputs is the ONE
     * root of a plan with several consumers: children()[0] is the Result (the caller's stream), every further
     * child a sink root.
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
     * Whether the node's steps must drain their input before they can emit. This is the pipeline cut
     * point and it is NOT derivable from the other two: Sort is preserving+opaque+blocking, Write is
     * preserving+opaque+streaming, Collect is preserving+transparent+blocking.
     *
     * A node NodeTranslator turns into Transformers only is streaming by construction: Segment::execute()
     * calls transform() inside the per-batch loop, so a Transformer cannot buffer the stream. Only a
     * Processor - handed the Generator - can be blocking.
     */
    public function materialization(): Materialization;

    /**
     * Columns this node introduces or renames on its output. A predicate that references one of them means a
     * different column below this node, so it cannot be pushed past it. Answered from what the node holds,
     * never from a class list.
     */
    public function redefines(): Redefined;
}
