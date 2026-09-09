<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use function sprintf;

final readonly class Fence
{
    public function __construct(
        public string $file,
        public int $line,
        public FenceInfo $info,
        public string $code,
    ) {}

    public function location(): string
    {
        return sprintf('%s:%d', $this->file, $this->line);
    }
}
