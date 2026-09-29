<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function in_array;
use function sprintf;

final class CollectUnique implements AggregatingFunction
{
    use ResolvesFromChildren;

    /**
     * @var array<mixed>
     */
    private array $collection;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_collection_unique';
        $this->collection = [];
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
            if (!in_array($value, $this->collection, true)) {
                $this->collection[] = $value;
            }
        }
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($other->collection as $value) {
            if (!in_array($value, $this->collection, true)) {
                $this->collection[] = $value;
            }
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
        return type_optional(type_list($this->ref->returns()));
    }

    /**
     * @return array<mixed>
     */
    public function value(): array
    {
        return $this->collection;
    }
}
