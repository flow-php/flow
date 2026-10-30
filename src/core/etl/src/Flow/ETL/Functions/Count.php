<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
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
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_structure;

final class Count implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    private int $count;

    private ?Column $column = null;

    /**
     * @var null|WeakReference<Rows>
     */
    private ?WeakReference $rows = null;

    private readonly string $outputName;

    public function __construct(
        private readonly ?Reference $ref = null,
        private readonly ?Window $window = null,
    ) {
        if ($ref === null) {
            $this->outputName = '_count';
        } else {
            $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_count';
        }
        $this->count = 0;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            ($data['ref'] ?? null) === null ? null : $rebuild->of($data['ref'], Reference::class),
            ($data['window'] ?? null) === null ? null : $rebuild->of($data['window'], Window::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['ref' => $this->ref, 'window' => $this->window]);
    }

    public function merge(array $partial, Backend $backend): void
    {
        $this->count += type_integer()->assert($this->partialType()->assert($partial)['count']);
    }

    public function partial(): array
    {
        return ['count' => $this->count];
    }

    public function partialType(): StructureType
    {
        return type_structure(['count' => type_integer()]);
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return $this->ref === null ? [] : [$this->ref];
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
        if ($children === []) {
            if ($this->ref !== null) {
                throw InvalidLogicException::because(
                    'count() has a reference child; withChildren() must receive it back.',
                );
            }

            return new self(null, $this->window);
        }

        if ($this->ref === null) {
            throw InvalidLogicException::because(
                'count() has no reference child; withChildren() must receive an empty list.',
            );
        }

        /** @var list<Reference> $children */
        return new self($children[0], $this->window);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        if ($this->ref === null) {
            $this->count += count($indices);

            return;
        }

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
            $this->count += $this->column->isNull($indices[0]) ? 0 : 1;

            return;
        }

        $this->count += count($indices) - $this->column->take($indices)->nullCount();
    }

    public function accumulator(): self
    {
        return new self($this->ref);
    }

    public function values(Rows $partition, Backend $backend): array
    {
        return (new FrameAccumulation($this->window()->frame()))->values($this, $partition, $backend);
    }

    public function over(Window $window): static
    {
        return new self($this->ref, $window);
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
        return $this->ref === null ? [] : [$this->ref];
    }

    /**
     * NOT NULL - an empty group counts to 0, never to null.
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function value(): int
    {
        return $this->count;
    }

    public function toString(): string
    {
        return 'count()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
