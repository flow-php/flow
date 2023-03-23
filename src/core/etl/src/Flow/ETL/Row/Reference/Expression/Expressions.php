<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Expressions implements Expression
{
    /**
     * @var array<Expression>
     */
    private array $expressions;

    /**
     * @param string $entry
     * @param array<Expression> $expressions
     */
    public function __construct(
        private readonly string $entry,
        array $expressions
    ) {
        $this->expressions = $expressions;
    }

    public function add(Expression $expression) : self
    {
        return new self($this->entry, \array_merge($this->expressions, [$expression]));
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    public function eval(Row $row) : mixed
    {
        $lastValue = null;

        foreach ($this->expressions as $expression) {
            $lastValue = $expression->eval($row);
            $row = $row->set((new Row\Factory\NativeEntryFactory())->create($this->entry, $lastValue));
        }

        return $lastValue;
    }

    public function literal() : ?Literal
    {
        $expression = \end($this->expressions);

        return $expression instanceof Literal ? $expression : null;
    }
}
