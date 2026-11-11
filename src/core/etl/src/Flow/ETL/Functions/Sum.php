<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Calculator\RunningSum;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Window\FrameAccumulation;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedTotals;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_fill;
use function count;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;

final class Sum implements AggregatesNumericTotal, FrameAccumulating, WindowFunction
{
    private readonly string $outputName;

    private readonly NumericTotal $total;

    private readonly LastBatchColumn $column;

    public function __construct(
        private readonly Reference $ref,
        private readonly bool $exact = false,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_sum';
        $this->total = new NumericTotal($exact);
        $this->column = new LastBatchColumn($ref->base());
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['ref'] ?? null, Reference::class),
            type_boolean()->assert($data['exact'] ?? null),
            ($data['window'] ?? null) === null ? null : $rebuild->of($data['window'], Window::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
            'exact' => $this->exact,
            'window' => $this->window,
        ]);
    }

    public function merge(array $partial, Backend $backend): void
    {
        $this->total->merge($partial);
    }

    public function partial(): array
    {
        return $this->total->partial();
    }

    public function partialType(): StructureType
    {
        return NumericTotal::partialType();
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [$this->ref];
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

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var array{0: Reference} $children */
        return new self($children[0], $this->exact, $this->window);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        $column = $this->column->of($rows);

        if ($column !== null) {
            $this->total->aggregate($column, $indices);
        }
    }

    public function exact(): bool
    {
        return $this->exact;
    }

    public function frameValues(Rows $partition, array $bounds, Backend $backend, Progress $progress): array
    {
        $column = $this->column->of($partition);

        if ($column === null) {
            return array_fill(0, count($bounds), null);
        }

        $values = [];

        foreach (NumericTotal::frames($column->values(), $bounds, $this->exact, $progress)['sums'] as $sum) {
            $values[] = $sum === null ? null : (float) $sum;
        }

        return $values;
    }

    public function values(Rows $partition, Backend $backend, Progress $progress): array
    {
        return (new FrameAccumulation($this->window()->frame()))->values($this, $partition, $backend, $progress);
    }

    public function over(Window $window): static
    {
        return new self($this->ref, $this->exact, $window);
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function outputName(): string
    {
        return $this->outputName;
    }

    /**
     * float, not the argument type - RunningSum promotes to float on int overflow, so integer
     * would be a declaration the accumulator can violate on ordinary data
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function groupValues(GroupedTotals $totals, Backend $backend): Column
    {
        return $totals->sums();
    }

    public function value(): ?float
    {
        $sum = $this->total->sum();

        return $sum === null ? null : (float) $sum;
    }

    public function toString(): string
    {
        return 'sum()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
