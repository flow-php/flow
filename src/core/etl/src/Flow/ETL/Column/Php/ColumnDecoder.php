<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\NullType;

use function sprintf;
use function strlen;

final readonly class ColumnDecoder
{
    /**
     * @param Type<mixed> $type
     * @param int $nullCount -1 derives the null count from the validity buffer
     *
     * @throws InvalidArgumentException
     */
    public function decode(Type $type, Buffers $buffers, int $count, int $nullCount): Column
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        if ($base instanceof NullType) {
            if ($nullCount !== -1 && $nullCount !== $count) {
                throw new InvalidArgumentException(sprintf(
                    'Null column of %d rows carries a null count of %d',
                    $count,
                    $nullCount,
                ));
            }

            return new ConstantColumn($type, new NullPhysical(), null, $count);
        }

        $validity = new Validity();
        $bitmap = $buffers->next();
        $valid = $validity->valid($bitmap, $count);
        $derived = $validity->nullCount($bitmap, $count);

        if ($nullCount !== -1 && $nullCount !== $derived) {
            throw new InvalidArgumentException(sprintf(
                'Column null count %d disagrees with its validity bitmap (%d nulls in %d rows)',
                $nullCount,
                $derived,
                $count,
            ));
        }

        if ($base instanceof ListType) {
            $offsets = (new Offsets())->unpack($buffers->next(), $count, 'List');

            return new ListColumn(
                $type,
                $offsets,
                $this->decode($base->element(), $buffers, $offsets[$count], -1),
                $validity->nulls($valid),
            );
        }

        if ($base instanceof MapType) {
            $offsets = (new Offsets())->unpack($buffers->next(), $count, 'Map');
            $entries = $buffers->next();

            if ($entries !== '') {
                throw new InvalidArgumentException(sprintf(
                    'Map entries validity must be omitted, got %d bytes',
                    strlen($entries),
                ));
            }

            return new MapColumn(
                $type,
                $offsets,
                $this->decode($base->key(), $buffers, $offsets[$count], -1),
                $this->decode($base->value(), $buffers, $offsets[$count], -1),
                $validity->nulls($valid),
            );
        }

        if ($base instanceof StructureType) {
            $children = [];

            foreach ($base->elements() as $element) {
                $children[$element->name] = $this->decode($element->type, $buffers, $count, -1);
            }

            return new StructColumn($type, $children, $validity->nulls($valid));
        }

        return new ScalarColumn(
            $type,
            (new PhysicalFor())->type($type),
            (new LayoutFor())
                ->type($type)
                ->decode($buffers, $count, $valid),
            $derived,
        );
    }
}
