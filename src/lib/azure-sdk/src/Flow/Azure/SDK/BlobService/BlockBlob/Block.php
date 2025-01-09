<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\BlockBlob;

final readonly class Block
{
    public function __construct(public string $id, public BlockState $state, public ?int $size = null)
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
