<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Exception\InvalidArgumentException;
use SplObjectStorage;

use function count;
use function in_array;

final readonly class ReplaceNodes implements Rewrite
{
    /**
     * Targets are found by the identity they have before the rewrite.
     *
     * @param SplObjectStorage<Node, Node> $replacements target => replacement, which takes the target's rewritten
     *                                                   children
     * @param list<Node> $removals nodes of one child, each giving way to its rewritten child
     */
    public function __construct(
        private SplObjectStorage $replacements,
        private array $removals = [],
    ) {
        foreach ($removals as $removal) {
            if ($replacements->offsetExists($removal)) {
                throw new InvalidArgumentException(
                    'A node is either replaced or removed, ' . $removal::class . ' is both',
                );
            }
        }
    }

    public static function one(Node $target, Node $replacement): self
    {
        /** @var SplObjectStorage<Node, Node> $replacements */
        $replacements = new SplObjectStorage();
        $replacements[$target] = $replacement;

        return new self($replacements);
    }

    public function of(Node $original, Node $rebuilt): Node
    {
        if (in_array($original, $this->removals, true)) {
            return count($rebuilt->children()) === 1
                ? $rebuilt->children()[0]
                : throw new InvalidArgumentException(
                    'Only a node of one child gives way to it, ' . $original::class . ' has '
                        . count($rebuilt->children()),
                );
        }

        return $this->replacements->offsetExists($original)
            ? $this->replacements[$original]->withChildren($rebuilt->children())
            : $rebuilt;
    }
}
