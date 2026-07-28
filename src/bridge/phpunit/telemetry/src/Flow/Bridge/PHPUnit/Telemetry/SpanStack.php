<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Tracer\Span;
use SplStack;

final class SpanStack
{
    /**
     * @var \SplStack<array{span: Span, scope: Scope}>
     */
    private SplStack $stack;

    /**
     * @var array<string, Span>
     */
    private array $suiteSpans = [];

    public function __construct()
    {
        /** @var \SplStack<array{span: Span, scope: Scope}> $stack */
        $stack = new SplStack();
        $this->stack = $stack;
    }

    public function clear(): void
    {
        /** @var \SplStack<array{span: Span, scope: Scope}> $stack */
        $stack = new SplStack();
        $this->stack = $stack;
        $this->suiteSpans = [];
    }

    public function current(): ?Span
    {
        if ($this->stack->isEmpty()) {
            return null;
        }

        return $this->stack->top()['span'];
    }

    public function getSuiteSpan(string $suiteName): ?Span
    {
        return $this->suiteSpans[$suiteName] ?? null;
    }

    public function isEmpty(): bool
    {
        return $this->stack->isEmpty();
    }

    /**
     * Detaches the entry's context scope before returning it, so the stack owns the LIFO ordering that
     * Scope::detach() requires.
     */
    public function pop(): ?Span
    {
        if ($this->stack->isEmpty()) {
            return null;
        }

        $entry = $this->stack->pop();
        $entry['scope']->detach();

        return $entry['span'];
    }

    public function push(Span $span, Scope $scope): void
    {
        $this->stack->push(['span' => $span, 'scope' => $scope]);
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
