<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Google\Protobuf\Internal\Message;

use function count;

/**
 * Provides context information during AST modification.
 *
 * This class gives modifiers access to the traversal state, including
 * parent nodes and depth in the tree, enabling context-aware modifications.
 */
final readonly class ModificationContext
{
    /**
     * @param list<Message> $ancestors Messages from the root statement down to the parent, excluding Node wrappers
     * @param int $depth Current depth in the AST: root statements are at 1, +1 per message edge
     * @param ParseResult $parseResult The full parsed AST for context-aware operations
     */
    public function __construct(
        private array $ancestors,
        private int $depth,
        private ParseResult $parseResult,
    ) {}

    /**
     * @return list<Message>
     */
    public function ancestors(): array
    {
        return $this->ancestors;
    }

    public function depth(): int
    {
        return $this->depth;
    }

    public function isTopLevel(): bool
    {
        return $this->depth === 1;
    }

    public function parent(): ?Message
    {
        return $this->ancestors[count($this->ancestors) - 1] ?? null;
    }

    public function parseResult(): ParseResult
    {
        return $this->parseResult;
    }
}
