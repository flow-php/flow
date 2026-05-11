<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Telemetry\Tracer\Span;

final class SpanStack
{
    /**
     * @var \SplStack<Span>
     */
    private \SplStack $stack;

    /**
     * @var array<string, Span>
     */
    private array $suiteSpans = [];

    public function __construct()
    {
        /** @var \SplStack<Span> $stack */
        $stack = new \SplStack();
        $this->stack = $stack;
    }

    public function clear(): void
    {
        /** @var \SplStack<Span> $stack */
        $stack = new \SplStack();
        $this->stack = $stack;
        $this->suiteSpans = [];
    }

    public function current(): ?Span
    {
        if ($this->stack->isEmpty()) {
            return null;
        }

        return $this->stack->top();
    }

    public function getSuiteSpan(string $suiteName): ?Span
    {
        return $this->suiteSpans[$suiteName] ?? null;
    }

    public function isEmpty(): bool
    {
        return $this->stack->isEmpty();
    }

    public function pop(): ?Span
    {
        if ($this->stack->isEmpty()) {
            return null;
        }

        return $this->stack->pop();
    }

    public function push(Span $span): void
    {
        $this->stack->push($span);
    }

    public function removeSuiteSpan(string $suiteName): void
    {
        unset($this->suiteSpans[$suiteName]);
    }

    public function setSuiteSpan(string $suiteName, Span $span): void
    {
        $this->suiteSpans[$suiteName] = $span;
    }
}
