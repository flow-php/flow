<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Exception\Exception as TypesException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\UnionType;

use function count;
use function Flow\Types\DSL\type_string;
use function get_debug_type;

/**
 * Resolves the concrete member Type of a union column for a given value.
 */
final readonly class EntryTypeResolver
{
    /**
     * A union is resolved to the first member accepting the value, falling back to the first
     * member the value can be cast to, mirroring UnionType::cast().
     *
     * @param Type<mixed>|UnionType<mixed, mixed> $type
     *
     * @throws InvalidArgumentException when the value matches no union member
     *
     * @return Type<mixed>
     */
    public function fromUnion(UnionType|Type $type, mixed $value, string $entryName): Type
    {
        /** @var UnionType<mixed, mixed> $type */
        $members = [];

        foreach ($type->types()->all() as $member) {
            if ($member instanceof OptionalType) {
                $member = $member->base();
            }

            if ($member instanceof NullType) {
                continue;
            }

            $members[] = $member;
        }

        if (!count($members)) {
            return type_string();
        }

        if ($value === null) {
            return $members[0];
        }

        $member = $type->memberFor($value);

        if ($member !== null) {
            return $member;
        }

        foreach ($members as $member) {
            try {
                $member->cast($value);

                return $member;
            } catch (TypesException) {
                continue;
            }
        }

        throw new InvalidArgumentException(
            "Entry \"{$entryName}\": "
            . get_debug_type($value)
            . " value does not match any member of union type \"{$type->toString()}\"",
        );
    }
}
