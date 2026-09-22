<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;

use function array_map;

final class SelectStmtContextSpy implements NodeModifier
{
    /**
     * @var list<list<class-string>>
     */
    public array $ancestors = [];

    /**
     * @var list<int>
     */
    public array $depths = [];

    /**
     * @var list<null|class-string>
     */
    public array $parents = [];

    public static function nodeClasses(): array
    {
        return [SelectStmt::class];
    }

    public function modify(object $node, ModificationContext $context): null
    {
        $this->depths[] = $context->depth();
        $this->ancestors[] = array_map(static fn(object $ancestor): string => $ancestor::class, $context->ancestors());
        $parent = $context->parent();
        $this->parents[] = $parent === null ? null : $parent::class;

        return null;
    }
}
