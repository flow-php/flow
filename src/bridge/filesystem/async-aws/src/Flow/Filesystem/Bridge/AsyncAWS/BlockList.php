<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

final class BlockList
{
    /**
     * @var array<int, string>
     */
    private array $blocks = [];

    public function add(string $blockETag) : self
    {
        $this->blocks[\count($this->blocks) + 1] = $blockETag;

        return $this;
    }

    public function count() : int
    {
        return \count($this->blocks);
    }

    /**
     * @return array<array{PartNumber: int, ETag: string}>
     */
    public function toArray() : array
    {
        $array = [];

        foreach ($this->blocks as $partNumber => $blockETag) {
            $array[] = ['PartNumber' => $partNumber, 'ETag' => $blockETag];
        }

        return $array;
    }
}
