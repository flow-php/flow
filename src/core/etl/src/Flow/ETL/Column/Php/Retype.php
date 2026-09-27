<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
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

        $fromBase = $from instanceof OptionalType ? $from->base() : $from;
        $toBase = $to instanceof OptionalType ? $to->base() : $to;

        $sameKind = match (true) {
            $fromBase::class !== $toBase::class => false,
            $fromBase instanceof EnumType && $toBase instanceof EnumType => $fromBase->class === $toBase->class,
            $fromBase instanceof StructureType && $toBase instanceof StructureType => array_map(
                static fn(StructureElement $element): int|string => $element->name,
                $fromBase->elements(),
            ) === array_map(static fn(StructureElement $element): int|string => $element->name, $toBase->elements()),
            default => true,
        };

        if (!$sameKind) {
            throw new InvalidArgumentException(sprintf(
                '%s and %s are different column kinds',
                $from->toString(),
                $to->toString(),
            ));
        }
    }
}
