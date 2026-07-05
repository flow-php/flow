<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JsonSchema;

final readonly class ResolvedReference
{
    /**
     * @param array<string, mixed> $schema resolved subschema
     * @param array<string, mixed> $document document containing the subschema, used to resolve internal pointers
     * @param string $baseUri base URI for resolving relative references inside the subschema
     * @param string $identity absolute identity of the reference target, used for cycle detection
     */
    public function __construct(
        public array $schema,
        public array $document,
        public string $baseUri,
        public string $identity,
    ) {}
}
