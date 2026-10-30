<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\Rounding;
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
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_float;
use function is_int;
use function is_numeric;
use function is_string;

final class Average implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    private int $count = 0;

    private readonly string $outputName;

    private ?RunningSum $runningSum = null;

    private ?Column $column = null;

    /**
     * @var null|WeakReference<Rows>
     */
    private ?WeakReference $rows = null;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale = 2,
        private readonly Rounding $rounding = Rounding::HALF_UP,
        private readonly bool $exact = false,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_avg';
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['ref'] ?? null, Reference::class),
            type_integer()->assert($data['scale'] ?? null),
            type_enum(Rounding::class)->assert(constant(
                Rounding::class . '::' . type_string()->assert($data['rounding'] ?? null),
            )),
            type_boolean()->assert($data['exact'] ?? null),
            ($data['window'] ?? null) === null ? null : $rebuild->of($data['window'], Window::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
            'scale' => $this->scale,
            'rounding' => $this->rounding->name,
            'exact' => $this->exact,
            'window' => $this->window,
        ]);
    }

    public function merge(array $partial, Backend $backend): void
    {
        $data = $this->partialType()->assert($partial);
        $count = type_integer()->assert($data['count']);

        if ($count === 0) {
            return;
        }

        $this->runningSum ??= new RunningSum(new Calculator());
        $this->runningSum->merge(
            RunningSum::fromPartial(type_array()->assert($data['sum']), new Calculator()),
            $this->exact,
        );
        $this->count += $count;
    }

    public function partial(): array
    {
        return [
            'count' => $this->count,
            'sum' => $this->runningSum?->partial() ?? RunningSum::emptyPartial(),
        ];
    }

    public function partialType(): StructureType
    {
        return type_structure([
            'count' => type_integer(),
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
        /** @var list<Reference> $children */
        return new self($children[0], $this->scale, $this->rounding, $this->exact, $this->window);
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
                $this->count++;
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
        $this->count += count($values);
    }

    public function accumulator(): self
    {
        return new self($this->ref, $this->scale, $this->rounding, $this->exact);
    }

    public function values(Rows $partition, Backend $backend): array
    {
        return (new FrameAccumulation($this->window()->frame()))->values($this, $partition, $backend);
    }

    public function over(Window $window): static
    {
        return new self($this->ref, $this->scale, $this->rounding, $this->exact, $window);
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
        return type_optional(type_float());
    }

    public function value(): ?float
    {
        if (0 === $this->count || $this->runningSum === null) {
            return null;
        }

        return (new Calculator())->divide($this->runningSum->value(), $this->count, $this->scale, $this->rounding);
    }

    public function toString(): string
    {
        return 'average()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
