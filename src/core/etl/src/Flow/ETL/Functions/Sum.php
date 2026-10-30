<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\RunningSum;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Window\FrameAccumulation;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;
use WeakReference;

use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function is_numeric;

final class Sum implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    private int $aggregated = 0;

    private readonly string $outputName;

    private ?RunningSum $runningSum = null;

    private ?Column $column = null;

    /**
     * @var null|WeakReference<Rows>
     */
    private ?WeakReference $rows = null;

    public function __construct(
        private readonly Reference $ref,
        private readonly bool $exact = false,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_sum';
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
        $data = $this->partialType()->assert($partial);
        $aggregated = type_integer()->assert($data['aggregated']);

        if ($aggregated === 0) {
            return;
        }

        $this->runningSum ??= new RunningSum(new Calculator());
        $this->runningSum->merge(
            RunningSum::fromPartial(type_array()->assert($data['sum']), new Calculator()),
            $this->exact,
        );
        $this->aggregated += $aggregated;
    }

    public function partial(): array
    {
        return [
            'aggregated' => $this->aggregated,
            'sum' => $this->runningSum?->partial() ?? RunningSum::emptyPartial(),
        ];
    }

    public function partialType(): StructureType
    {
        return type_structure([
            'aggregated' => type_integer(),
            'sum' => RunningSum::partialType(),
        ]);
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
        // a window frame calls one instance with the same partition per row: resolve its column once. Weak, so a
        // group-by aggregate never keeps the last batch it saw alive
        if ($this->rows?->get() !== $rows) {
            $this->rows = WeakReference::create($rows);
            $this->column = $rows->schema()->findDefinition($this->ref->base()) === null
                ? null
                : $rows->column($this->ref->base());
        }

        if ($this->column === null) {
            return;
        }

        if (count($indices) === 1) {
            // @mago-ignore analysis:mixed-assignment
            $value = $this->column->value($indices[0]);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->runningSum ??= new RunningSum(new Calculator());
                $this->runningSum->add($value, $this->exact);
                $this->aggregated++;
            }

            return;
        }

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->column->take($indices)->values() as $value) {
            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $values[] = $value;
            }
        }

        if ($values === []) {
            return;
        }

        $this->runningSum ??= new RunningSum(new Calculator());
        $this->runningSum->addAll($values, $this->exact);
        $this->aggregated += count($values);
    }

    public function accumulator(): self
    {
        return new self($this->ref, $this->exact);
    }

    public function values(Rows $partition, Backend $backend): array
    {
        return (new FrameAccumulation($this->window()->frame()))->values($this, $partition, $backend);
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

    public function value(): ?float
    {
        if ($this->aggregated === 0 || $this->runningSum === null) {
            return null;
        }

        return (float) $this->runningSum->value();
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
