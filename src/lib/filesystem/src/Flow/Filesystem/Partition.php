<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\TypedValueFormatter;

use function array_filter;
use function array_keys;
use function explode;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_string;
use function preg_match;
use function rawurldecode;
use function rawurlencode;
use function sprintf;
use function strlen;

final class Partition
{
    /**
     * A null partition value has no directory name of its own, so it borrows Hive's, which every
     * engine that reads a Hive tree already understands.
     */
    public const string NULL_VALUE = '__HIVE_DEFAULT_PARTITION__';

    public function __construct(
        public readonly string $name,
        public readonly ?string $value,
    ) {
        if ('' === $this->name) {
            throw new InvalidArgumentException("Partition name can't be empty");
        }

        if ('' === $this->value) {
            throw new InvalidArgumentException("Partition value can't be empty");
        }
    }

    /**
     * Percent-encoding over RFC 3986's unreserved set - `A-Za-z0-9_-.~`, which is the set DuckDB uses -
     * so a value carrying `/` or `=` becomes a directory name instead of being refused outright.
     */
    public static function decode(string $segment): string
    {
        return rawurldecode($segment);
    }

    public static function encode(string $value): string
    {
        return rawurlencode($value);
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array<Partition>
     */
    public static function fromArray(array $data): array
    {
        $partitions = [];

        foreach (array_keys($data) as $partition) {
            $partitions[] = new self(
                $partition,
                $data[$partition] === null ? null : type_string()->cast($data[$partition]),
            );
        }

        return $partitions;
    }

    /**
     * @return null|self null when the segment is not a `name=value` pair at all
     */
    public static function fromSegment(string $segment): ?self
    {
        $matches = [];

        if (!preg_match('/^([^=]+)=([^=]+)$/', $segment, $matches)) {
            return null;
        }

        return new self(self::decode($matches[1]), $matches[2] === self::NULL_VALUE ? null : self::decode($matches[2]));
    }

    public static function fromUri(string $uri): Partitions
    {
        $partitions = [];

        foreach (array_filter(explode('/', $uri), static fn(string $s): bool => (bool) strlen($s)) as $uriPart) {
            $partition = self::fromSegment($uriPart);

            if ($partition !== null) {
                $partitions[] = $partition;
            }
        }

        return new Partitions(...$partitions);
    }

    /**
     * @param Type<mixed> $type
     */
    public static function fromValue(string $name, Type $type, mixed $value): string
    {
        if ($type instanceof DateTimeType) {
            return type_datetime()->assert($value)->format('Y-m-d');
        }

        if (
            $type instanceof HTMLType
            || $type instanceof HTMLElementType
            || $type instanceof XMLType
            || $type instanceof XMLElementType
            || $type instanceof JsonType
            || $type instanceof ListType
            || $type instanceof StructureType
            || $type instanceof MapType
        ) {
            throw new InvalidArgumentException(sprintf(
                'Column "%s" of type %s can\'t be used as a partition',
                $name,
                $type->toString(),
            ));
        }

        return (new TypedValueFormatter())->format($type, $value);
    }

    public function id(): string
    {
        return $this->name . '|' . ($this->value ?? self::NULL_VALUE);
    }

    /**
     * The `name=value` directory segment. Every path built from partitions goes through here, so the
     * encoding on write and the decoding on read cannot drift apart.
     */
    public function segment(): string
    {
        return (
            self::encode($this->name) . '=' . ($this->value === null ? self::NULL_VALUE : self::encode($this->value))
        );
    }
}
