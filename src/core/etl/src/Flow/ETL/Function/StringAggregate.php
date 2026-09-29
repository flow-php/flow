<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Rows;
use Flow\ETL\Sort\RowOrder;
use Flow\Types\Type;

use function count;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_string;
use function rsort;
use function sort;
use function sprintf;

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

    public function aggregate(Rows $rows, array $indices, FlowContext $context): void
    {
        if ($rows->schema()->findDefinition($this->ref->to()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->to())->take($indices)->values() as $value) {
            if (is_string($value)) {
                $this->values[] = $value;
            }
        }
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        $this->values = [...$this->values, ...$other->values];
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
            $flag = RowOrder::flag(type_string());
            $this->sort === SortOrder::ASC ? sort($values, $flag) : rsort($values, $flag);
        }

        return implode($this->separator, $values);
    }
}
