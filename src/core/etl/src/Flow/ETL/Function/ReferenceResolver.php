<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema;

final readonly class ReferenceResolver
{
    /**
     * A new tree whose every reachable UnresolvedReference is replaced by a ResolvedReference carrying
     * its Type and nullability. Never mutates $function.
     * Total: a reference the schema does not know is left unresolved, not thrown on.
     *
     * One pass, not a fixpoint - this resolver inserts nothing, so a post-order pass over a finite
     * tree converges in one traversal by construction. The day a coercion pass inserts Cast nodes
     * into this tree, the pass count must be re-decided.
     *
     * Sound: the only non-leaf return is $function->withChildren(), typed static; a root that is
     * itself a reference leaf resolves to its Reference sibling class, never to a foreign node.
     *
     * @template T of FunctionTree
     *
     * @param T $function
     *
     * @return (T is UnresolvedReference ? Reference : T)
     */
    public function resolve(FunctionTree $function, Schema $schema): FunctionTree
    {
        if ($function instanceof UnresolvedReference) {
            // Resolution is by source column - an alias renames the output, never the lookup
            // (resolve() carries the alias onto the ResolvedReference).
            $definition = $schema->findDefinition($function->to());

            return $definition === null ? $function : $function->resolve($definition);
        }

        $children = $function->children();

        if ($children === []) {
            return $function;
        }

        $resolved = [];

        foreach ($children as $child) {
            $resolved[] = $this->resolve($child, $schema);
        }

        // PHP === on arrays is element-wise and identity-comparing for objects, so a no-op pass
        // allocates nothing and returns the same object (Spark's childrenFastEquals).
        if ($resolved === $children) {
            return $function;
        }

        // @mago-ignore analysis:less-specific-return-statement - withChildren(): static preserves
        // the node's class, which the analyzer cannot unify with T.
        return $function->withChildren($resolved);
    }

    /**
     * The gate. Refuses to hand an unresolved tree to execution.
     *
     * @throws SchemaDefinitionNotFoundException naming the first unresolved column and the available ones
     */
    public function assertResolved(FunctionTree $function, Schema $schema): void
    {
        if ($function instanceof UnresolvedReference) {
            throw SchemaDefinitionNotFoundException::withAvailable(
                $function->name(),
                ...$schema->references()->names(),
            );
        }

        foreach ($function->children() as $child) {
            $this->assertResolved($child, $schema);
        }
    }
}
