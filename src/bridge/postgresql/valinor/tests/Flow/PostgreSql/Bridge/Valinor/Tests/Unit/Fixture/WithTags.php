<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture;

final readonly class WithTags
{
    /**
     * @param list<string> $tags
     */
    public function __construct(
        public int $id,
        public array $tags,
    ) {}
}
