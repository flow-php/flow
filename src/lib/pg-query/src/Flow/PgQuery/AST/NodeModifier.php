<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST;

/**
 * Interface for AST node modifiers.
 *
 * Modifiers can mutate nodes in-place during traversal.
 * Unlike visitors (which are read-only collectors), modifiers are expected to change the AST.
 */
interface NodeModifier
{
    /**
     * Don't traverse children of the current node.
     */
    public const DONT_TRAVERSE_CHILDREN = 1;

    /**
     * Stop the entire traversal.
     */
    public const STOP_TRAVERSAL = 2;

    /**
     * Returns the fully qualified class name of the node type this modifier handles.
     *
     * @return class-string The node class this modifier is registered for
     */
    public static function nodeClass() : string;

    /**
     * Called to modify a node of the registered type.
     *
     * The modifier can:
     * - Mutate the node in-place and return null to continue traversal
     * - Return DONT_TRAVERSE_CHILDREN to skip child nodes
     * - Return STOP_TRAVERSAL to stop the entire traversal
     * - Return a new node to replace the current node (used for wrapping operations)
     *
     * @param object $node The node instance to modify (type depends on nodeClass())
     * @param ModificationContext $context Context providing parent information
     *
     * @return null|int|object
     *                         - null: Continue traversal (node unchanged or modified in-place)
     *                         - int (DONT_TRAVERSE_CHILDREN, STOP_TRAVERSAL): Control flow
     *                         - object: Replace current node with returned node
     */
    public function modify(object $node, ModificationContext $context) : int|object|null;
}
