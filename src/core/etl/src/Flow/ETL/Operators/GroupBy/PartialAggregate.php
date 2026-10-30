<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

/**
 * Aggregates like $aggregate; its output column holds partial(), so wave 1 of a shuffle reuses the group-by
 * processor unchanged.
 */
final class PartialAggregate implements AggregatingFunction
{
    public function __construct(
        private readonly AggregatingFunction $aggregate,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['aggregate'] ?? null, AggregatingFunction::class));
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        $this->aggregate->aggregate($rows, $indices, $backend);
    }

    public function children(): array
    {
        return $this->aggregate->children();
    }

    public function deterministic(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->deterministic()) {
                return false;
            }
        }

        return true;
    }

    public function resolved(): bool
    {
        foreach ($this->children() as $child) {
            if (!$child->resolved()) {
                return false;
            }
        }

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

    public function references(): ?array
    {
        return $this->aggregate->references();
    }

    public function returns(): Type
    {
        return $this->aggregate->partialType();
    }

    public function value(): array
    {
        return $this->aggregate->partial();
    }

    public function withChildren(array $children): static
    {
        return new self($this->aggregate->withChildren($children));
    }
}
