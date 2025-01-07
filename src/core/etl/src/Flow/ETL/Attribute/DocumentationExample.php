<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

#[\Attribute(\Attribute::IS_REPEATABLE | \Attribute::TARGET_FUNCTION)]
final readonly class DocumentationExample
{
    public function __construct(
        public string $topic,
        public string $example,
    ) {
    }
}
