<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{array_entry: string, path: string, new_name: string}>
 */
final class ArrayKeyRename implements Serializable
{
    public function __construct(private string $arrayEntry, private string $path, private string $newName)
    {
    }

    public function __serialize() : array
    {
        return [
            'array_entry' => $this->arrayEntry,
            'path' => $this->path,
            'new_name' => $this->newName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntry = 'array_entry';
        $this->path = 'path';
        $this->newName = 'new_name';
    }

    public function arrayEntry() : string
    {
        return $this->arrayEntry;
    }

    public function newName() : string
    {
        return $this->newName;
    }

    public function path() : string
    {
        return $this->path;
    }
}
