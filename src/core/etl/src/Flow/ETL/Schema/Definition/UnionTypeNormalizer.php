<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\UnionType;

use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_optional;

final readonly class UnionTypeNormalizer
{
    /**
     * @param UnionType<mixed, mixed> $type
     *
     * @return UnionType<mixed, mixed>
     */
    public function normalize(UnionType $type): UnionType
    {
        $members = [];
        $changed = false;

        foreach ($type->types()->all() as $member) {
            $normalizedMember = match (true) {
                $member instanceof ArrayType => type_json(),
                $member instanceof OptionalType && $member->base() instanceof ArrayType => type_optional(type_json()),
                default => $member,
            };

            if ($normalizedMember !== $member) {
                $changed = true;
            }

            $members[] = $normalizedMember;
        }

        if (!$changed) {
            return $type;
        }

        $union = null;

        foreach ($members as $member) {
            $union = $union === null ? $member : new UnionType($union, $member);
        }

        return $union instanceof UnionType ? $union : $type;
    }
}
