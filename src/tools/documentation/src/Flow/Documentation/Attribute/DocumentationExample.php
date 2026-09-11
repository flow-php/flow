<?php

declare(strict_types=1);

namespace Flow\Documentation\Attribute;

use Attribute;

#[Attribute(Attribute::IS_REPEATABLE | Attribute::TARGET_FUNCTION)]
final readonly class DocumentationExample
{
    public function __construct(
        public string $topic,
        public string $example,
        public ?string $option = null,
    ) {}
}
