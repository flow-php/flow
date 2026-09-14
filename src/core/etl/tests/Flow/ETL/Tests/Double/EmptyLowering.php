<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Plan\Node;
use Flow\ETL\Planner\Lowering;

/**
 * Lowers any node kind to no steps at all, so a test can register a kind the default registry does not
 * know yet - or a test-only node - and still walk it.
 *
 * @implements Lowering<Node>
 */
final readonly class EmptyLowering implements Lowering
{
    /**
     * @param class-string<Node> $kind
     */
    public function __construct(
        private string $kind,
    ) {}

    public function handles(): string
    {
        return $this->kind;
    }

    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return [];
    }
}
