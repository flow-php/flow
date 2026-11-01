<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

use Google\Protobuf\Internal\Message;

interface NodeVisitor
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
     * Returns the fully qualified class names of the node types this visitor handles.
     *
     * A visitor may handle more than one node type; the traverser will dispatch it
     * for every type listed here.
     *
     * @return list<class-string<Message>> The node classes this visitor is registered for
     */
    public static function nodeClasses(): array;

    /**
     * Called when entering a node of the registered type.
     *
     * @param object $node The node instance (one of the types listed in nodeClasses())
     *
     * @return null|int Return value determines traversal behavior:
     *                  - null: Continue traversal
     *                  - DONT_TRAVERSE_CHILDREN: Don't traverse children
     *                  - STOP_TRAVERSAL: Stop entire traversal
     */
    public function enter(object $node): ?int;

    /**
     * Called when leaving a node of the registered type.
     *
     * @param object $node The node instance (one of the types listed in nodeClasses())
     *
     * @return null|int Return value determines traversal behavior:
     *                  - null: Continue traversal
     *                  - STOP_TRAVERSAL: Stop entire traversal
     */
    public function leave(object $node): ?int;
}
