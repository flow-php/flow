<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Closure;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function count;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final readonly class ContainerUnification
{
    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     * @param Closure(Type<mixed>, Type<mixed>): ?Type<mixed> $pairwise
     * @param Closure(Type<mixed>, Type<mixed>): ?Type<mixed> $strictPairwise
     *
     * @return null|Type<mixed>
     */
    public function unify(Type $left, Type $right, Closure $pairwise, Closure $strictPairwise): ?Type
    {
        if ($left instanceof OptionalType || $right instanceof OptionalType) {
            $base = $pairwise(
                $left instanceof OptionalType ? $left->base() : $left,
                $right instanceof OptionalType ? $right->base() : $right,
            );

            if ($base === null) {
                return null;
            }

            return $base instanceof OptionalType ? $base : type_optional($base);
        }

        if ($left instanceof ListType && $right instanceof ListType) {
            $element = $pairwise($left->element(), $right->element());

            return $element === null ? null : type_list($element);
        }

        if ($left instanceof MapType && $right instanceof MapType) {
            $key = $strictPairwise($left->key(), $right->key());

            if ($key === null || (new Nullability())->is($key)) {
                return null;
            }

            $value = $pairwise($left->value(), $right->value());

            if ($value === null) {
                return null;
            }

            // U-04a.11 permits integer ⊔ float keys to unify to float, which the array-key template
            // cannot express - the nullable-key guard above is the runtime gate that matters.
            // @mago-expect analysis:template-constraint-violation
            // @mago-expect analysis:less-specific-nested-argument-type
            return type_map($key, $value);
        }

        if ($left instanceof StructureType && $right instanceof StructureType) {
            $leftOptional = $left->optionalElements();
            $rightOptional = $right->optionalElements();
            $leftElements = $left->elements() + $leftOptional;
            $rightElements = $right->elements() + $rightOptional;

            if (count($leftElements) !== count($rightElements)) {
                return null;
            }

            $required = [];
            $optional = [];

            foreach ($leftElements as $name => $leftElement) {
                if (!array_key_exists($name, $rightElements)) {
                    return null;
                }

                $element = $pairwise($leftElement, $rightElements[$name]);

                if ($element === null) {
                    return null;
                }

                if (array_key_exists($name, $leftOptional) || array_key_exists($name, $rightOptional)) {
                    $optional[$name] = $element;
                } else {
                    $required[$name] = $element;
                }
            }

            return type_structure($required, $optional, $left->allowsExtra() || $right->allowsExtra());
        }

        return null;
    }
}
