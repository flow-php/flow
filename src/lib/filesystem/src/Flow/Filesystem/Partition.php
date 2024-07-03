<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\{ArrayEntry,
    DateTimeEntry,
    JsonEntry,
    ListEntry,
    MapEntry,
    ObjectEntry,
    StructureEntry,
    XMLElementEntry,
    XMLEntry};
use Flow\ETL\Row\{EntryReference, Reference};

final class Partition
{
    /**
     * @var array<string>
     */
    private static array $forbiddenCharacters = ['/', '\\', '=', ':', '>', '<', '|', '"', '?', '*'];

    public function __construct(public readonly string $name, public readonly string $value)
    {
        if ('' === $this->name) {
            throw new InvalidArgumentException("Partition name can't be empty");
        }

        if ('' === $this->value) {
            throw new InvalidArgumentException("Partition value can't be empty");
        }

        $regex = '/^([^\/\\\=:><|"?*]+)$/';

        if (!\preg_match($regex, $this->name)) {
            throw new InvalidArgumentException("Partition name contains one of forbidden characters: ['" . \implode("', '", self::$forbiddenCharacters) . "']");
        }

        if (!\preg_match($regex, $this->value)) {
            throw new InvalidArgumentException("Partition value contains one of forbidden characters: ['" . \implode("', '", self::$forbiddenCharacters) . "']");
        }
    }

    /**
     * @param array<string, mixed> $data
     *
     * @return array<Partition>
     */
    public static function fromArray(array $data) : array
    {
        $partitions = [];

        foreach ($data as $partition => $value) {
            $partitions[] = new self($partition, (string) $value);
        }

        return $partitions;
    }

    public static function fromUri(string $uri) : Partitions
    {
        $regex = '/^([^\/\\\=:><|"?*]+)=([^\/\\\=:><|"?*]+)$/';

        $partitions = [];

        foreach (\array_filter(\explode('/', $uri), static fn (string $s) : bool => (bool) \strlen($s)) as $uriPart) {
            if (\preg_match($regex, $uriPart, $matches)) {
                $partitions[] = new self($matches[1], $matches[2]);
            }
        }

        return new Partitions(...$partitions);
    }

    public static function valueFromRow(Reference $ref, Row $row) : mixed
    {
        $entry = $row->get($ref);

        return match ($entry::class) {
            DateTimeEntry::class => $entry->value()?->format('Y-m-d'),
            XMLEntry::class, XMLElementEntry::class, JsonEntry::class, ObjectEntry::class, ListEntry::class, StructureEntry::class, MapEntry::class, ArrayEntry::class => throw new InvalidArgumentException($entry::class . ' can\'t be used as a partition'),
            default => $entry->toString(),
        };
    }

    public function id() : string
    {
        return $this->name . '|' . $this->value;
    }

    public function reference() : Reference
    {
        return new EntryReference($this->name);
    }
}
