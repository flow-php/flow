<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Row\Entry\XMLNodeEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{name: string, value: string}>
 */
final class Partition implements Serializable
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

        foreach (\array_filter(\explode('/', $uri), 'strlen') as $uriPart) {
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
            DateTimeEntry::class => $entry->value()->format('Y-m-d'),
            XMLEntry::class, XMLNodeEntry::class, JsonEntry::class, ObjectEntry::class, ListEntry::class, StructureEntry::class, MapEntry::class, NullEntry::class, ArrayEntry::class => throw new InvalidArgumentException($entry::class . ' can\'t be used as a partition'),
            default => $entry->toString(),
        };
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->value = $data['value'];
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
