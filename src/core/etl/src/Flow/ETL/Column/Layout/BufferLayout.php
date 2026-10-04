<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Layout;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\NullType;

use function array_push;
use function array_slice;
use function count;
use function sprintf;
use function strlen;
use function unpack;

/**
 * The one layout of Column::encode(): the pre-order nodes and buffers a column type spans.
 */
final readonly class BufferLayout
{
    /**
     * @param Type<mixed> $type
     */
    public function nodeCount(Type $type): int
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        if ($base instanceof ListType) {
            return 1 + $this->nodeCount($base->element());
        }

        if ($base instanceof MapType) {
            return 2 + $this->nodeCount($base->key()) + $this->nodeCount($base->value());
        }

        if ($base instanceof StructureType) {
            $nodes = 1;

            foreach ($base->elements() as $element) {
                $nodes += $this->nodeCount($element->type);
            }

            return $nodes;
        }

        return 1;
    }

    /**
     * @param Type<mixed> $type
     */
    public function bufferCount(Type $type): int
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        if ($base instanceof NullType) {
            return 0;
        }

        if ($base instanceof ListType) {
            return 2 + $this->bufferCount($base->element());
        }

        if ($base instanceof MapType) {
            return 3 + $this->bufferCount($base->key()) + $this->bufferCount($base->value());
        }

        if ($base instanceof StructureType) {
            $buffers = 1;

            foreach ($base->elements() as $element) {
                $buffers += $this->bufferCount($element->type);
            }

            return $buffers;
        }

        return (new LayoutFor())->type($type) instanceof Utf8Layout ? 3 : 2;
    }

    /**
     * Pre-order {length, nullCount} of one column's tree: the first node as given, every child derived from the
     * buffers - a null kind is {length, length}, list and map children span the last offset, a map entries node
     * holds no nulls and structure children span their parent.
     *
     * @param Type<mixed> $type
     * @param list<string> $buffers
     *
     * @throws InvalidArgumentException
     *
     * @return list<array{int, int}>
     */
    public function nodes(Type $type, int $length, int $nullCount, array $buffers): array
    {
        if (count($buffers) !== $this->bufferCount($type)) {
            throw new InvalidArgumentException(sprintf(
                'Column of type %s holds %d buffers, its layout needs %d',
                $type->toString(),
                count($buffers),
                $this->bufferCount($type),
            ));
        }

        $base = $type instanceof OptionalType ? $type->base() : $type;
        $nodes = [[$length, $nullCount]];
        $children = [];
        $position = 1;

        if ($base instanceof ListType || $base instanceof MapType) {
            if (strlen($buffers[1]) !== (($length + 1) * 4)) {
                throw new InvalidArgumentException(sprintf(
                    '%s offsets buffer of %d bytes, expected %d for %d rows',
                    $base instanceof ListType ? 'List' : 'Map',
                    strlen($buffers[1]),
                    ($length + 1) * 4,
                    $length,
                ));
            }

            /** @var int $childLength */
            $childLength = unpack('V', $buffers[1], $length * 4)[1];

            if ($base instanceof ListType) {
                $children[] = [$base->element(), $childLength];
                $position = 2;
            } else {
                if ($buffers[2] !== '') {
                    throw new InvalidArgumentException(sprintf(
                        'Map entries validity must be omitted, got %d bytes',
                        strlen($buffers[2]),
                    ));
                }

                $nodes[] = [$childLength, 0];
                $children[] = [$base->key(), $childLength];
                $children[] = [$base->value(), $childLength];
                $position = 3;
            }
        } elseif ($base instanceof StructureType) {
            foreach ($base->elements() as $element) {
                $children[] = [$element->type, $length];
            }
        }

        foreach ($children as [$childType, $childLength]) {
            $childBuffers = array_slice($buffers, $position, $this->bufferCount($childType));
            $position += count($childBuffers);
            $childBase = $childType instanceof OptionalType ? $childType->base() : $childType;

            array_push($nodes, ...$this->nodes(
                $childType,
                $childLength,
                $childBase instanceof NullType
                    ? $childLength
                    : (new Validity())->nullCount($childBuffers[0], $childLength),
                $childBuffers,
            ));
        }

        return $nodes;
    }
}
