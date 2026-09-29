<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function count;
use function Flow\Types\DSL\type_optional;
use function sprintf;

final class Last implements AggregatingFunction
{
    use ResolvesFromChildren;

    private bool $found = false;

    /**
     * @var null|array<array-key, mixed>|bool|float|int|object|string
     */
    private mixed $last;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_last';
        $this->last = null;
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
        if ($indices === [] || $rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-property-type-coercion
        $this->last = $rows->column($this->ref->base())->value($indices[count($indices) - 1]);
        $this->found = true;
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        if (!$other->found) {
            return;
        }

        $this->last = $other->last;
        $this->found = true;
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
        return type_optional($this->ref->returns());
    }

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function value(): mixed
    {
        return $this->last;
    }
}
