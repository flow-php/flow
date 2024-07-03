<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\BlockBlob;

final class Block
{
    public function __construct(public readonly string $id, public readonly BlockState $state, public readonly ?int $size = null)
    {

    }

    public static function commited(string $id) : self
    {
        return new self($id, BlockState::COMMITTED);
    }

    public static function latest(string $id) : self
    {
        return new self($id, BlockState::LATEST);
    }

    public static function uncommited(string $id) : self
    {
        return new self($id, BlockState::UNCOMMITTED);
    }
}
