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
use function explode;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_string;
use function implode;
use function preg_match;
use function sprintf;
use function strlen;

final class Partition
{
    /**
     * @var array<string>
     */
    private static array $forbiddenCharacters = ['/', '\\', '=', ':', '>', '<', '|', '"', '?', '*', '{', '}'];

    public function __construct(
        public readonly string $name,
        public readonly string $value,
    ) {
        if ('' === $this->name) {
            throw new InvalidArgumentException("Partition name can't be empty");
        }

        if ('' === $this->value) {
            throw new InvalidArgumentException("Partition value can't be empty");
        }

        $regex = '/^([^\/\\\=:><|"?*{}]+)$/';

        if (!preg_match($regex, $this->name)) {
            throw new InvalidArgumentException(
                "Partition name contains one of forbidden characters: ['"
                . implode("', '", self::$forbiddenCharacters)
                . "']",
            );
        }

        if (!preg_match($regex, $this->value)) {
            throw new InvalidArgumentException(
                "Partition value contains one of forbidden characters: ['"
                . implode("', '", self::$forbiddenCharacters)
                . "']",
            );
        }
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
            $partitions[] = new self($partition, type_string()->cast($data[$partition]));
        }

        return $partitions;
    }

    public static function fromUri(string $uri): Partitions
    {
        $regex = '/^([^\/\\\=:><|"?*{}]+)=([^\/\\\=:><|"?*{}]+)$/';

        $partitions = [];
        $matches = [];

        foreach (array_filter(explode('/', $uri), static fn(string $s): bool => (bool) strlen($s)) as $uriPart) {
            if (preg_match($regex, $uriPart, $matches)) {
                $partitions[] = new self($matches[1], $matches[2]);
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
        return $this->name . '|' . $this->value;
    }
}
