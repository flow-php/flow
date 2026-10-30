<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function count;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final class Last implements AggregatingFunction
{
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
        $data = $this->partialType()->assert($partial);

        if (type_boolean()->assert($data['found'])) {
            // @mago-ignore analysis:mixed-property-type-coercion
            $this->last = $data['value'];
            $this->found = true;
        }
    }

    public function partial(): array
    {
        return ['value' => $this->last, 'found' => $this->found];
    }

    public function partialType(): StructureType
    {
        return type_structure(['value' => type_optional(type_bare($this->ref->returns())), 'found' => type_boolean()]);
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
        if ($indices === [] || $rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-property-type-coercion
        $this->last = $rows->column($this->ref->base())->value($indices[count($indices) - 1]);
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
