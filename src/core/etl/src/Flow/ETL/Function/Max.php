<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\Types\DSL\type_optional;
use function is_numeric;
use function max;
use function sprintf;

final class Max implements AggregatingFunction
{
    use ResolvesFromChildren;

    private float|DateTimeInterface|null $max;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_max';
        $this->max = null;
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
        return new self($children[0]);
    }

    public function aggregate(Rows $rows, array $indices, FlowContext $context): void
    {
        if ($rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->base())->take($indices)->values() as $value) {
            if ($this->max === null) {
                if (is_numeric($value)) {
                    $this->max = (float) $value;
                } elseif ($value instanceof DateTimeInterface) {
                    $this->max = $value;
                }
            } else {
                if (is_numeric($value)) {
                    $this->max = max($this->max, (float) $value);
                } elseif ($value instanceof DateTimeInterface) {
                    $this->max = max($this->max, $value);
                }
            }
        }
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        if ($other->max === null) {
            return;
        }

        $this->max = $this->max === null ? $other->max : max($this->max, $other->max);
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
     * Exactly the argument type, nullable - an all-null or ref-less group leaves no maximum
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->ref->returns());
    }

    public function value(): float|DateTimeInterface|null
    {
        return $this->max;
    }
}
