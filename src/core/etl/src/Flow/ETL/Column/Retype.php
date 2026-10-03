<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\EnumType;

use function array_map;
use function sprintf;

final readonly class Retype
{
    /**
     * @param Type<mixed> $from
     * @param Type<mixed> $to
     *
     * @throws InvalidArgumentException
     */
    public function assert(Type $from, Type $to, int $nullCount): void
    {
        if ($from instanceof OptionalType && !$to instanceof OptionalType && $nullCount > 0) {
            throw new InvalidArgumentException(sprintf('%d null values under a required type', $nullCount));
        }

        if (!$this->sameKind($from, $to)) {
            throw new InvalidArgumentException(sprintf(
                '%s and %s are different column kinds',
                $from->toString(),
                $to->toString(),
            ));
        }
    }

    /**
     * Whether withType($to) on a column of $from can only fail on top-level nulls: the same kind at every level, and no
     * nested level goes from optional to required (list element, map key / value, structure element by name -
     * optional flag and type).
     *
     * @param Type<mixed> $from
     * @param Type<mixed> $to
     */
    public function proves(Type $from, Type $to): bool
    {
        if (!$this->sameKind($from, $to)) {
            return false;
        }

        $fromBase = $from instanceof OptionalType ? $from->base() : $from;
        $toBase = $to instanceof OptionalType ? $to->base() : $to;

        if ($fromBase instanceof ListType && $toBase instanceof ListType) {
            return $this->provesNested($fromBase->element(), $toBase->element());
        }

        if ($fromBase instanceof MapType && $toBase instanceof MapType) {
            return (
                $this->provesNested($fromBase->key(), $toBase->key())
                && $this->provesNested($fromBase->value(), $toBase->value())
            );
        }

        if ($fromBase instanceof StructureType && $toBase instanceof StructureType) {
            foreach ($toBase->elements() as $element) {
                $own = $fromBase->element($element->name);

                if (
                    $own === null
                    || $own->optional && !$element->optional
                    || !$this->provesNested($own->type, $element->type)
                ) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * proves() one level down, where a null cannot be counted: optional to required is never proven.
     *
     * @param Type<mixed> $from
     * @param Type<mixed> $to
     */
    public function provesNested(Type $from, Type $to): bool
    {
        return !($from instanceof OptionalType && !$to instanceof OptionalType) && $this->proves($from, $to);
    }

    /**
     * The column kind rule: the same type class, an enum of the same class, a structure of the same element names.
     *
     * @param Type<mixed> $from
     * @param Type<mixed> $to
     */
    public function sameKind(Type $from, Type $to): bool
    {
        $fromBase = $from instanceof OptionalType ? $from->base() : $from;
        $toBase = $to instanceof OptionalType ? $to->base() : $to;

        return match (true) {
            $fromBase::class !== $toBase::class => false,
            $fromBase instanceof EnumType && $toBase instanceof EnumType => $fromBase->class === $toBase->class,
            $fromBase instanceof StructureType && $toBase instanceof StructureType => array_map(
                static fn(StructureElement $element): int|string => $element->name,
                $fromBase->elements(),
            ) === array_map(static fn(StructureElement $element): int|string => $element->name, $toBase->elements()),
            default => true,
        };
    }
}
