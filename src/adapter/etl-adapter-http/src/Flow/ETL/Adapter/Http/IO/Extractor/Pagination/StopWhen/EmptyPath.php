<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;

final class EmptyPath implements StopWhen
{
    public function __construct(
        private readonly string $path,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        $body = $response->body();

        return !array_dot_exists($body, $this->path) || array_dot_get($body, $this->path) === [];
    }
}
