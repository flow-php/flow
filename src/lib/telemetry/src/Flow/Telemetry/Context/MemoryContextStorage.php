<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

use function array_key_exists;
use function array_key_last;
use function array_splice;

final class MemoryContextStorage implements ContextStorage
{
    private readonly Context $root;

    /**
     * One entry per attach; the root context is held separately so the stack can drain to empty.
     *
     * @var array<int, Context>
     */
    private array $stack = [];

    public function __construct(?Context $context = null)
    {
        $this->root = $context ?? Context::root();
    }

    public function attach(Context $context): Scope
    {
        $this->stack[] = $context;

        return new ContextScope(array_key_last($this->stack), $this);
    }

    public function current(): Context
    {
        $last = array_key_last($this->stack);

        return $last === null ? $this->root : $this->stack[$last];
    }

    /**
     * Detaching out of order still restores, per the OTEL context spec - the return value is the only signal.
     *
     * @internal
     */
    public function detachAt(int $depth): int
    {
        if (!array_key_exists($depth, $this->stack)) {
            return Scope::INACTIVE;
        }

        $mismatch = $depth !== array_key_last($this->stack);

        array_splice($this->stack, $depth);

        return $mismatch ? Scope::MISMATCH : Scope::DETACHED;
    }
}
