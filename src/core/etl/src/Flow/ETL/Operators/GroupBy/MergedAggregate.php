<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

/**
 * Reads the column $aggregate->outputName() as partials and merges them; its output is $aggregate's value(), so
 * wave 2 of a shuffle and the parent's finisher reuse the group-by processor unchanged.
 */
final class MergedAggregate implements AggregatingFunction
{
    /**
     * @param AggregatingFunction $aggregate resolved against the plan's input schema
     */
    public function __construct(
        private readonly AggregatingFunction $aggregate,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['aggregate'] ?? null, AggregatingFunction::class));
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        $column = $rows->column($this->aggregate->outputName());

        foreach ($indices as $index) {
            $this->aggregate->merge(type_map(type_string(), type_mixed())->assert($column->value($index)), $backend);
        }
    }

    /**
     * The inner aggregator is resolved already; the resolver must not touch it.
     */
    public function children(): array
    {
        return [];
    }

    public function deterministic(): bool
    {
        return true;
    }

    public function resolved(): bool
    {
        return true;
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['aggregate' => $this->aggregate]);
    }

    public function merge(array $partial, Backend $backend): void
    {
        $this->aggregate->merge($partial, $backend);
    }

    public function outputName(): string
    {
        return $this->aggregate->outputName();
    }

    public function partial(): array
    {
        return $this->aggregate->partial();
    }

    public function partialType(): StructureType
    {
        return $this->aggregate->partialType();
    }

    public function references(): array
    {
        return [new UnresolvedReference($this->aggregate->outputName())];
    }

    public function returns(): Type
    {
        return $this->aggregate->returns();
    }

    public function value(): mixed
    {
        return $this->aggregate->value();
    }

    /**
     * A fresh inner aggregator, as Aggregators::fresh() needs.
     */
    public function withChildren(array $children): static
    {
        return new self($this->aggregate->withChildren($this->aggregate->children()));
    }
}
