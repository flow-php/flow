<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

interface FunctionTree
{
    /**
     * Every operand of this node that is an expression, in constructor order.
     * Raw scalars, enums, Types and scope-introducing bodies are fields, not children.
     *
     * @return list<FunctionTree>
     */
    public function children(): array;

    /**
     * True when this node and every node in children() can answer returns().
     */
    public function resolved(): bool;

    /**
     * A copy of this node with $children in place of children(): same count, same order, same
     * per-element narrowing. Never mutates $this.
     *
     * @internal count($children) MUST equal count($this->children()). ReferenceResolver preserves this
     *           by construction; AllFunctionsDeclareTheirTypeTest is the enforcement.
     *
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static;
}
