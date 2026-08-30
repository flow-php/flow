<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\UnionType;

use function array_slice;
use function count;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_union;

final readonly class FieldOrderConformer
{
    /**
     * Reorder $subject's structure fields to $authority's wherever the name sets match. Total -
     * anything that is not a structure pair with a matching name set is returned unchanged.
     *
     * @param Type<mixed> $subject
     * @param Type<mixed> $authority
     *
     * @return Type<mixed>
     */
    public function conform(Type $subject, Type $authority): Type
    {
        if ($subject instanceof StructureType && $authority instanceof StructureType) {
            if (count($subject->elements()) !== count($authority->elements())) {
                return $subject;
            }

            $elements = [];
            $subjectByName = [];

            foreach ($subject->elements() as $subjectElement) {
                $subjectByName[$subjectElement->name] = $subjectElement;
            }

            foreach ($authority->elements() as $authorityElement) {
                $subjectElement = $subjectByName[$authorityElement->name] ?? null;

                if ($subjectElement === null) {
                    return $subject;
                }

                $elements[] = structure_element(
                    $subjectElement->name,
                    $this->conform($subjectElement->type, $authorityElement->type),
                    $subjectElement->optional,
                );
            }

            return new StructureType($elements, $subject->allowsExtra());
        }

        if ($subject instanceof ListType && $authority instanceof ListType) {
            return type_list($this->conform($subject->element(), $authority->element()));
        }

        if ($subject instanceof MapType && $authority instanceof MapType) {
            // conform() cannot change a key's array-key-ness - it only reorders structure fields -
            // but the recursion widens the static type to Type<mixed>.
            // @mago-expect analysis:template-constraint-violation
            // @mago-expect analysis:less-specific-nested-argument-type
            return type_map(
                $this->conform($subject->key(), $authority->key()),
                $this->conform($subject->value(), $authority->value()),
            );
        }

        if ($subject instanceof OptionalType && $authority instanceof OptionalType) {
            return type_optional($this->conform($subject->base(), $authority->base()));
        }

        if ($subject instanceof UnionType && $authority instanceof UnionType) {
            $subjectMembers = $subject->types()->all();
            $authorityMembers = $authority->types()->all();

            if (count($subjectMembers) !== count($authorityMembers) || count($subjectMembers) < 2) {
                return $subject;
            }

            $conformed = [];

            foreach ($subjectMembers as $position => $member) {
                $conformed[] = $this->conform($member, $authorityMembers[$position]);
            }

            return type_union($conformed[0], $conformed[1], ...array_slice($conformed, 2));
        }

        return $subject;
    }
}
