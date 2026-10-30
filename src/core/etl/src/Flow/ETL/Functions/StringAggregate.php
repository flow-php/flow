<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\SortOrder;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_values;
use function count;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function implode;
use function is_string;
use function rsort;
use function sort;

final class StringAggregate implements AggregatingFunction
{
    private readonly string $outputName;

    /**
     * @var array<string>
     */
    private array $values = [];

    public function __construct(
        private readonly Reference $ref,
        private readonly string $separator,
        private readonly ?SortOrder $sort = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_str_agg';
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        // @mago-ignore analysis:mixed-assignment
        $sort = $data['sort'] ?? null;

        return new self(
            $rebuild->of($data['ref'] ?? null, Reference::class),
            type_string()->assert($data['separator'] ?? null),
            $sort === null
                ? null
                : type_enum(SortOrder::class)->assert(constant(
                    SortOrder::class . '::' . type_string()->assert($data['sort'] ?? null),
                )),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'ref' => $this->ref,
            'separator' => $this->separator,
            'sort' => $this->sort?->name,
        ]);
    }

    public function merge(array $partial, Backend $backend): void
    {
        $this->values = [
            ...$this->values,
            ...type_list(type_string())->assert($this->partialType()->assert($partial)['values']),
        ];
    }

    public function partial(): array
    {
        return ['values' => array_values($this->values)];
    }

    public function partialType(): StructureType
    {
        return type_structure(['values' => type_list(type_string())]);
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
        return new self($children[0], $this->separator, $this->sort);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        if ($rows->schema()->findDefinition($this->ref->to()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->to())->take($indices)->values() as $value) {
            if (is_string($value)) {
                $this->values[] = $value;
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
        return type_optional(type_string());
    }

    public function value(): string
    {
        if (!count($this->values)) {
            return '';
        }

        $values = $this->values;

        if ($this->sort) {
            $flag = RowOrder::flag(type_string());
            $this->sort === SortOrder::ASC ? sort($values, $flag) : rsort($values, $flag);
        }

        return implode($this->separator, $values);
    }
}
