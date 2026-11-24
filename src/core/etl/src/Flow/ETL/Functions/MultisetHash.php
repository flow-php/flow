<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Hash\HashAlgorithm;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_map;
use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class MultisetHash implements AggregatesByGroup
{
    private ?GroupedMultisetHashes $hash = null;

    private MultisetDigest $absorbed;

    /**
     * @param list<Reference> $refs [] hashes every column
     */
    public function __construct(
        private readonly array $refs = [],
    ) {
        $this->absorbed = MultisetDigest::empty();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(array_values($rebuild->all($data['refs'] ?? [], Reference::class)));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['refs' => $this->refs]);
    }

    public function aggregate(Rows $rows, array $indices, Backend $backend): void
    {
        if ($indices === []) {
            return;
        }

        $names = $this->names();
        $columns = $names === [] ? $rows->columns() : $rows->select(...$names)->columns();
        ($this->hash ??= $backend->groupedMultisetHashes(HashAlgorithm::xxh128))->add(
            count($indices) === $rows->count()
                ? $columns
                : array_map(static fn(Column $column): Column => $column->take($indices), $columns),
            $backend->constant(Definitions::of('group', type_integer()), 0, count($indices)),
        );
    }

    public function merge(array $partial, Backend $backend): void
    {
        $this->absorbed = $this->absorbed->plus(MultisetDigest::fromHex(type_string()->assert(
            $partial['hash'] ?? null,
        )));
    }

    public function partial(): array
    {
        return ['hash' => $this->value()];
    }

    public function partialType(): StructureType
    {
        return type_structure(['hash' => type_string()]);
    }

    public function grouped(Backend $backend): GroupedAggregation
    {
        return new MultisetHashesByGroup($this->names(), $backend->groupedMultisetHashes(HashAlgorithm::xxh128));
    }

    /**
     * @return list<Reference>
     */
    public function children(): array
    {
        return $this->refs;
    }

    public function deterministic(): bool
    {
        foreach ($this->refs as $ref) {
            if (!$ref->deterministic()) {
                return false;
            }
        }

        return true;
    }

    public function resolved(): bool
    {
        foreach ($this->refs as $ref) {
            if (!$ref->resolved()) {
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
        if (count($children) !== count($this->refs)) {
            throw InvalidLogicException::because(
                'multiset_hash() has %d reference children; withChildren() must receive them back.',
                count($this->refs),
            );
        }

        /** @var list<Reference> $children */
        return new self($children);
    }

    public function outputName(): string
    {
        return '_multiset_hash';
    }

    /**
     * @return null|list<Reference> null keeps every column, because a multiset_hash() without references hashes them all
     */
    public function references(): ?array
    {
        return $this->refs === [] ? null : $this->refs;
    }

    /**
     * @return Type<string>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function value(): string
    {
        return MultisetDigest::fromHex(type_string()->assert(
            $this->hash?->hashes()->physicals()[0] ?? MultisetDigest::empty()->hex(),
        ))->plus($this->absorbed)->hex();
    }

    /**
     * @return list<string>
     */
    private function names(): array
    {
        return array_map(static fn(Reference $ref): string => $ref->base(), $this->refs);
    }
}
