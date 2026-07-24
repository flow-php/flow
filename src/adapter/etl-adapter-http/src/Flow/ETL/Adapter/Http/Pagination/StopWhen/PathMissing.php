<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen;

use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use function in_array;

final class PathMissing extends StopWhen
{
    public function __construct(
        private readonly string $path,
    ) {}

    public function shouldStop(DecodedResponse $response, PageState $state): bool
    {
        $body = $response->body();

        return !array_dot_exists($body, $this->path) || in_array(array_dot_get($body, $this->path), [null, ''], true);
    }
}
