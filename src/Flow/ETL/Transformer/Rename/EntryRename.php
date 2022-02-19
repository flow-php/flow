<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
final class EntryRename implements Serializable
{
    private string $from;

    private string $to;

    public function __construct(string $from, string $to)
    {
        $this->from = $from;
        $this->to = $to;
    }

    /**
     * @return array{from: string, to: string}
     */
    public function __serialize() : array
    {
        return [
            'from' => $this->from,
            'to' => $this->to,
        ];
    }

    /**
     * @param array{from: string, to: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->from = $data['from'];
        $this->to = $data['to'];
    }

    /**
     * @return string
     */
    public function from() : string
    {
        return $this->from;
    }

    /**
     * @return string
     */
    public function to() : string
    {
        return $this->to;
    }
}
