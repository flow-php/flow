<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

/**
 * Interface for AST node visitors.
 *
 * Visitors are registered for specific node types and only receive nodes of that type.
 * Use the static nodeClass() method to declare which node type this visitor handles.
 */
interface NodeVisitor
{
    /**
     * Don't traverse children of the current node.
     */
    public const DONT_TRAVERSE_CHILDREN = 1;

    /**
     * Remove the node from its parent array.
     */
    public const REMOVE_NODE = 3;

    /**
     * Stop the entire traversal.
     */
    public const STOP_TRAVERSAL = 2;

    /**
     * Returns the fully qualified class name of the node type this visitor handles.
     *
     * @return class-string The node class this visitor is registered for
     */
    public static function nodeClass() : string;

    /**
     * Called when entering a node of the registered type.
     *
     * @param object $node The node instance (type depends on nodeClass())
     *
     * @return null|int Return value determines traversal behavior:
     *                  - null: Continue traversal
     *                  - DONT_TRAVERSE_CHILDREN: Don't traverse children
     *                  - STOP_TRAVERSAL: Stop entire traversal
     */
    public function enter(object $node) : ?int;

    /**
     * Called when leaving a node of the registered type.
     *
     * @param object $node The node instance (type depends on nodeClass())
     *
     * @return null|int Return value determines traversal behavior:
     *                  - null: Continue traversal
     *                  - REMOVE_NODE: Remove node from parent
     *                  - STOP_TRAVERSAL: Stop entire traversal
     */
    public function leave(object $node) : ?int;
}
