<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ResolvesFromChildren;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function array_slice;
use function array_values;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function is_float;
use function is_int;
use function sprintf;

/**
 * A sum that also reads function operands, so its columns cannot be enumerated - the case that disables spill
 * column pruning.
 */
final class UnenumerableSum implements AggregatingFunction
{
    use ResolvesFromChildren;

    public ?float $total = null;

    /**
     * @var list<ScalarFunction>
     */
    public readonly array $operands;

    public function __construct(
        public readonly Reference $ref,
        ScalarFunction ...$operands,
    ) {
        $this->operands = array_values($operands);
    }

    public function aggregate(Rows $rows, array $indices, FlowContext $context): void
    {
        if ($rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->base())->take($indices)->values() as $value) {
            if (is_int($value) || is_float($value)) {
                $this->total = ($this->total ?? 0.0) + $value;
            }
        }
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref, ...$this->operands];
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        if ($other->total !== null) {
            $this->total = ($this->total ?? 0.0) + $other->total;
        }
    }

    public function outputName(): string
    {
        return $this->ref->to() . '_sum';
    }

    public function references(): ?array
    {
        return null;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function value(): ?float
    {
        return $this->total;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $operands */
        $operands = array_slice($children, 1);

        return new self(type_instance_of(Reference::class)->assert($children[0]), ...$operands);
    }
}
