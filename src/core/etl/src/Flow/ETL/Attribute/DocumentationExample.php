<?php

declare(strict_types=1);

namespace Flow\ETL\Attribute;

#[\Attribute(\Attribute::IS_REPEATABLE | \Attribute::TARGET_FUNCTION)]
final class DocumentationExample
{
    public function __construct(
        public readonly string $topic,
        public readonly string $example,
    ) {
    }
}
