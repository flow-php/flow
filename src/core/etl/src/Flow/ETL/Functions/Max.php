<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeInterface;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function is_numeric;
use function max;

final class Max implements AggregatingFunction
{
    private float|DateTimeInterface|null $max;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_max';
        $this->max = null;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['ref'] ?? null, Reference::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['ref' => $this->ref]);
    }

    public function merge(array $partial, Backend $backend): void
    {
        /** @var mixed $value */
        $value = $this->partialType()->assert($partial)['value'];

        if ($value === null) {
            return;
        }

        $candidate = is_numeric($value) ? (float) $value : type_instance_of(DateTimeInterface::class)->assert($value);
        $this->max = $this->max === null ? $candidate : max($this->max, $candidate);
    }

    public function partial(): array
    {
        return ['value' => $this->max === null ? null : type_bare($this->ref->returns())->cast($this->max)];
    }

    public function partialType(): StructureType
    {
        return type_structure(['value' => type_optional(type_bare($this->ref->returns()))]);
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
        return new self($children[0]);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
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
