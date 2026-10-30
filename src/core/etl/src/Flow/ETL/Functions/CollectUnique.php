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

use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function in_array;

final class CollectUnique implements AggregatingFunction
{
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
        // @mago-ignore analysis:mixed-assignment
        foreach (type_array()->assert($this->partialType()->assert($partial)['values']) as $value) {
            if (!in_array($value, $this->collection, true)) {
                $this->collection[] = $value;
            }
        }
    }

    public function partial(): array
    {
        return ['values' => array_values($this->collection)];
    }

    public function partialType(): StructureType
    {
        return type_structure(['values' => type_list($this->ref->returns())]);
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
