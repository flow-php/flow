<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Visitors;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\ParamRef;

/**
 * A visitor that collects all ParamRef (parameter reference) nodes.
 *
 * Useful for finding the maximum parameter number used in a query,
 * which is needed for keyset pagination when merging user parameters
 * with cursor values.
 */
final class ParamRefCollector implements NodeVisitor
{
    /**
     * @var array<ParamRef>
     */
    private array $paramRefs = [];

    public static function nodeClasses(): array
    {
        return [ParamRef::class];
    }

    public function enter(object $node): ?int
    {
        /** @var ParamRef $node */
        $this->paramRefs[] = $node;

        return null;
    }

    public function getMaxParamNumber(): int
    {
        if ($this->paramRefs === []) {
            return 0;
        }

        return \max(\array_map(static fn(ParamRef $ref): int => $ref->getNumber(), $this->paramRefs));
    }

    /**
     * @return array<ParamRef>
     */
    public function getParamRefs(): array
    {
        return $this->paramRefs;
    }

    public function leave(object $node): ?int
    {
        return null;
    }

    public function reset(): void
    {
        $this->paramRefs = [];
    }
}
