<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\Protobuf\AST\ResTarget;

final readonly class RemoveResTargetModifier implements NodeModifier
{
    public function __construct(
        private string $name,
    ) {}

    public static function nodeClasses(): array
    {
        return [ResTarget::class];
    }

    public function modify(object $node, ModificationContext $context): ?int
    {
        /** @var ResTarget $node */
        return $node->getName() === $this->name ? NodeModifier::REMOVE_NODE : null;
    }
}
