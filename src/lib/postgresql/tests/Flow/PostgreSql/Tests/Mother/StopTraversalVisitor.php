<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\RangeVar;

final class StopTraversalVisitor implements NodeVisitor
{
    public static function nodeClasses(): array
    {
        return [RangeVar::class];
    }

    public function enter(object $node): int
    {
        return NodeVisitor::STOP_TRAVERSAL;
    }

    public function leave(object $node): null
    {
        return null;
    }
}
