<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Visitors;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\RangeVar;

/**
 * A visitor that collects all RangeVar (table references) nodes.
 */
final class RangeVarCollector implements NodeVisitor
{
    /**
     * @var array<RangeVar>
     */
    private array $rangeVars = [];

    public static function nodeClasses() : array
    {
        return [RangeVar::class];
    }

    public function enter(object $node) : ?int
    {
        /** @var RangeVar $node */
        $this->rangeVars[] = $node;

        return null;
    }

    /**
     * @return array<RangeVar>
     */
    public function getRangeVars() : array
    {
        return $this->rangeVars;
    }

    public function leave(object $node) : ?int
    {
        return null;
    }

    public function reset() : void
    {
        $this->rangeVars = [];
    }
}
