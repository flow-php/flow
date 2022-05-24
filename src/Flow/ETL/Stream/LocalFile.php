<?php

declare(strict_types=1);

namespace Flow\ETL\Stream;

/**
 * @implements FileStream<array{path: string}>
 */
final class LocalFile implements FileStream
{
    public function __construct(private string $path)
    {
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
    }

    public function options() : array
    {
        return [];
    }

    public function scheme() : string
    {
        return 'file';
    }

    public function uri() : string
    {
        return $this->path;
    }
}
