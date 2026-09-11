<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

final readonly class ConditionBuilder
{
    private function __construct(
        private ?Condition $condition = null,
    ) {}

    public static function create(): self
    {
        return new self();
    }

    public function and(Condition|self $condition): self
    {
        $resolved = $condition instanceof self ? $condition->condition : $condition;

        if ($resolved === null) {
            return $this;
        }

        if ($this->condition === null) {
            return new self($resolved);
        }

        return new self($this->condition->and($resolved));
    }

    public function getCondition(): ?Condition
    {
        return $this->condition;
    }

    /**
     * @assert-if-true Condition $this->condition
     */
    public function isEmpty(): bool
    {
        return $this->condition === null;
    }

    public function or(Condition|self $condition): self
    {
        $resolved = $condition instanceof self ? $condition->condition : $condition;

        if ($resolved === null) {
            return $this;
        }

        if ($this->condition === null) {
            return new self($resolved);
        }

        return new self($this->condition->or($resolved));
    }
}
