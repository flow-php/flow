<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_slice;
use function array_values;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function is_float;
use function is_int;

/**
 * A sum that also reads function operands, so its columns cannot be enumerated - the case that disables spill
 * column pruning.
 */
final class UnenumerableSum implements AggregatingFunction
{
    public ?float $total = null;

    /**
     * @var list<ScalarFunction>
     */
    public readonly array $operands;

    public function __construct(
        public readonly Reference $ref,
        ScalarFunction ...$operands,
    ) {
        $this->operands = array_values($operands);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function merge(array $partial, Backend $backend): void
    {
        $total = type_optional(type_float())->assert($this->partialType()->assert($partial)['total']);

        if ($total !== null) {
            $this->total = ($this->total ?? 0.0) + $total;
        }
    }

    public function partial(): array
    {
        return ['total' => $this->total];
    }

    public function partialType(): StructureType
    {
        return type_structure(['total' => type_optional(type_float())]);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        if ($rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->base())->take($indices)->values() as $value) {
            if (is_int($value) || is_float($value)) {
                $this->total = ($this->total ?? 0.0) + $value;
            }
        }
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref, ...$this->operands];
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

    public function outputName(): string
    {
        return $this->ref->to() . '_sum';
    }

    public function references(): ?array
    {
        return null;
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
        return $this->total;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $operands */
        $operands = array_slice($children, 1);

        return new self(type_instance_of(Reference::class)->assert($children[0]), ...$operands);
    }
}
