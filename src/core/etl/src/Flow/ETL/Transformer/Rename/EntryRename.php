<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{from: string, to: string}>
 * @psalm-immutable
 */
final class EntryRename implements Serializable
{
    public function __construct(private string $from, private string $to)
    {
    }

    public function __serialize() : array
    {
        return [
            'from' => $this->from,
            'to' => $this->to,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->from = $data['from'];
        $this->to = $data['to'];
    }

    public function from() : string
    {
        return $this->from;
    }

    public function to() : string
    {
        return $this->to;
    }
}
