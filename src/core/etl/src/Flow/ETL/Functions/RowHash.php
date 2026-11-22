<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function array_map;
use function array_values;
use function Flow\Types\DSL\type_string;

final class RowHash implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var non-empty-list<Reference>
     */
    private readonly array $refs;

    /**
     * @param list<Reference> $refs
     *
     * @throws InvalidArgumentException
     */
    public function __construct(array $refs)
    {
        if ($refs === []) {
            throw new InvalidArgumentException(
                'row_hash() needs at least one column reference, for example row_hash(ref("id")).',
            );
        }

        $this->refs = $refs;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(array_values($rebuild->all($data['refs'] ?? null, Reference::class)));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['refs' => $this->refs]);
    }

    /**
     * @return non-empty-list<Reference>
     */
    public function children(): array
    {
        return $this->refs;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<Reference> $children */
        return new self($children);
    }

    /**
     * @return Type<string>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        return $backend->rowHashes(
            $rows->isEmpty()
                ? []
                : $rows->select(...array_map(
                    static fn(Reference $ref): string => $ref->base(),
                    $this->refs,
                ))->columns(),
            $rows->count(),
        );
    }
}
