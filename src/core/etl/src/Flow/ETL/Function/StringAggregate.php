<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\SortOrder;
use Flow\Types\Type;

use function count;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;
use function rsort;
use function sort;

final class StringAggregate implements AggregatingFunction
{
    use ResolvesFromChildren;

    private readonly string $outputName;

    /**
     * @var array<string>
     */
    private array $values = [];

    public function __construct(
        private readonly Reference $ref,
        private readonly string $separator,
        private readonly ?SortOrder $sort = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_str_agg';
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<Reference> $children */
        return new self($children[0], $this->separator, $this->sort);
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref->to())) {
            return;
        }

        $stringValue = $row->get($this->ref->to());

        if (is_string($stringValue)) {
            $this->values[] = $stringValue;
        }
    }

    public function outputName(): string
    {
        return $this->outputName;
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_string());
    }

    public function value(): string
    {
        if (!count($this->values)) {
            return '';
        }

        $values = $this->values;

        if ($this->sort) {
            $this->sort === SortOrder::ASC ? sort($values) : rsort($values);
        }

        return implode($this->separator, $values);
    }
}
