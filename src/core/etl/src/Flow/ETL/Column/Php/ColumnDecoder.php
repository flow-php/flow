<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\Layout\BufferLayout;
use Flow\ETL\Column\Layout\Buffers;
use Flow\ETL\Column\Layout\LayoutFor;
use Flow\ETL\Column\Layout\Offsets;
use Flow\ETL\Column\Layout\Validity;
use Flow\ETL\Column\Physical\NullPhysical;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\NullType;

use function sprintf;

final readonly class ColumnDecoder
{
    public function __construct(
        private BufferLayout $layout = new BufferLayout(),
    ) {}

    /**
     * Nested nulls follow Arrow: a list element or map value holds nulls only when optional or of the null kind, a map
     * key never, a structure child always may (its parent's nulls mask it).
     *
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    public function decode(Type $type, Buffers $buffers, int $count, int $nullCount): Column
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;
        $nodes = [];
        $own = $buffers;

        // only a container has child nodes to derive; a flat column reads its buffers straight off the cursor
        if ($base instanceof ListType || $base instanceof MapType || $base instanceof StructureType) {
            $subtree = [];

            for ($i = $this->layout->bufferCount($type); $i > 0; $i--) {
                $subtree[] = $buffers->next();
            }

            $nodes = $this->layout->nodes($type, $count, $nullCount, $subtree);
            $own = new Buffers($subtree);
        }

        if ($base instanceof NullType) {
            if ($nullCount !== $count) {
                throw new InvalidArgumentException(sprintf(
                    'Null column of %d rows carries a null count of %d',
                    $count,
                    $nullCount,
                ));
            }

            return new ConstantColumn($type, new NullPhysical(), null, $count);
        }

        $validity = new Validity();
        $bitmap = $own->next();
        $valid = $validity->valid($bitmap, $count);
        $derived = $validity->nullCount($bitmap, $count);

        if ($nullCount !== $derived) {
            throw new InvalidArgumentException(sprintf(
                'Column null count %d disagrees with its validity bitmap (%d nulls in %d rows)',
                $nullCount,
                $derived,
                $count,
            ));
        }

        if ($base instanceof ListType) {
            $offsets = (new Offsets())->unpack($own->next(), $count, 'List');
            $element = $base->element();
            [$length, $nulls] = $nodes[1];

            if ($nulls > 0 && !$element instanceof OptionalType && !$element instanceof NullType) {
                throw new InvalidArgumentException(sprintf(
                    'List child holds %d nulls in a non-nullable %s',
                    $nulls,
                    $element->toString(),
                ));
            }

            return new ListColumn(
                $type,
                $offsets,
                $this->decode($element, $own, $length, $nulls),
                $validity->nulls($valid),
            );
        }

        if ($base instanceof MapType) {
            $offsets = (new Offsets())->unpack($own->next(), $count, 'Map');
            $own->next();
            $key = $base->key();
            $value = $base->value();
            [$length, $keyNulls] = $nodes[2];
            [, $valueNulls] = $nodes[2 + $this->layout->nodeCount($key)];

            if ($keyNulls > 0) {
                throw new InvalidArgumentException(sprintf(
                    'Map key child holds %d nulls in a non-nullable %s',
                    $keyNulls,
                    $key->toString(),
                ));
            }

            if ($valueNulls > 0 && !$value instanceof OptionalType && !$value instanceof NullType) {
                throw new InvalidArgumentException(sprintf(
                    'Map value child holds %d nulls in a non-nullable %s',
                    $valueNulls,
                    $value->toString(),
                ));
            }

            return new MapColumn(
                $type,
                $offsets,
                $this->decode($key, $own, $length, $keyNulls),
                $this->decode($value, $own, $length, $valueNulls),
                $validity->nulls($valid),
            );
        }

        if ($base instanceof StructureType) {
            $children = [];
            $node = 1;

            foreach ($base->elements() as $element) {
                [$length, $nulls] = $nodes[$node];
                $node += $this->layout->nodeCount($element->type);
                $children[$element->name] = $this->decode($element->type, $own, $length, $nulls);
            }

            return new StructColumn($type, $children, $validity->nulls($valid));
        }

        return new ScalarColumn(
            $type,
            (new PhysicalFor())->type($type),
            (new LayoutFor())
                ->type($type)
                ->decode($own, $count, $valid),
            $derived,
        );
    }
}
