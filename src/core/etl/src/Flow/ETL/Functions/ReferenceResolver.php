<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_equals;
use function sprintf;

final readonly class ReferenceResolver
{
    /**
     * A new tree whose every reachable UnresolvedReference is replaced by a ResolvedReference carrying
     * its Type and nullability. Never mutates $function.
     * Total: a reference the schema does not know is left unresolved, not thrown on.
     *
     * One pass, not a fixpoint - the only nodes it inserts are the Casts TemporalStringCoercion wraps around
     * operands already resolved, and a Cast adds no reference, so a post-order pass over a finite tree
     * converges in one traversal.
     *
     * Sound: the only non-leaf returns are withChildren() and withOperands(), both typed static;
     * a root that is itself a reference leaf resolves to its Reference sibling class, never to a foreign node.
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

        // @mago-ignore analysis:less-specific-return-statement - withChildren(): static preserves
        // the node's class, which the analyzer cannot unify with T.
        return (new TemporalStringCoercion())->coerce(
            $resolved === $children ? $function : $function->withChildren($resolved),
        );
    }

    /**
     * resolve(), assertResolved(), refuse an array_expand() in $method, and refuse a result whose bare type is not
     * $returns when given.
     *
     * @template T of FunctionTree
     *
     * @param T $function
     * @param null|Type<mixed> $returns
     *
     * @throws SchemaDefinitionNotFoundException
     * @throws InvalidArgumentException
     *
     * @return (T is UnresolvedReference ? Reference : T)
     */
    public function bind(FunctionTree $function, Schema $schema, string $method, ?Type $returns = null): FunctionTree
    {
        $resolved = $this->resolve($function, $schema);
        $this->assertResolved($resolved, $schema);
        (new ExpandingFunctions())->refuse($resolved, $method);

        // type_bare() keeps the gate blind to nullability - a null-propagating predicate declares ?boolean
        if (
            $returns !== null
            && $resolved instanceof ScalarFunction
            && !type_equals(type_bare($resolved->returns()), $returns)
        ) {
            throw new InvalidArgumentException(sprintf(
                '%s() requires a predicate returning %s, "%s" returns "%s". '
                . 'Use an explicit comparison, e.g. ->notEquals(lit(0)).',
                $method,
                $returns->toString(),
                $resolved::class,
                $resolved->returns()->toString(),
            ));
        }

        return $resolved;
    }

    /**
     * The gate. Refuses to hand an unresolved tree to execution.
     *
     * @throws SchemaDefinitionNotFoundException naming the first unresolved column and the available ones
     */
    public function assertResolved(FunctionTree $function, Schema $schema): void
    {
        if ($function instanceof UnresolvedReference) {
            throw SchemaDefinitionNotFoundException::withAvailable($function->name(), ...$schema->names());
        }

        foreach ($function->children() as $child) {
            $this->assertResolved($child, $schema);
        }
    }
}
