<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParamRef;

final readonly class ReplaceColumnRefModifier implements NodeModifier
{
    public function __construct(
        private ?string $column = null,
    ) {}

    public static function nodeClasses(): array
    {
        return [ColumnRef::class];
    }

    public function modify(object $node, ModificationContext $context): ?Node
    {
        /** @var ColumnRef $node */
        if ($this->column !== null && $node->getFields()[0]->getString()?->getSval() !== $this->column) {
            return null;
        }

        return (new Node())->setParamRef((new ParamRef())->setNumber(99));
    }
}
