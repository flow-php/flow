<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\PaginationStrategy;
use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\Pagination\ResultCounter;
use Psr\Http\Message\RequestInterface;

use function count;
use function end;
use function Flow\ArrayDot\array_dot_exists;
use function Flow\ArrayDot\array_dot_get;
use function is_array;

final class LastRecordCursor implements PaginationStrategy, ResultCounter
{
    public function __construct(
        private readonly string $recordPath,
        private readonly RequestOption $inject,
    ) {}

    public function countResults(DecodedResponse $response): int
    {
        return count($this->records($response));
    }

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        return $base;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        $records = $this->records($previous);

        if (count($records) === 0) {
            return null;
        }

        /** @var array<mixed>|scalar|null $last */
        $last = end($records);

        if ($last === null || $last === '') {
            return null;
        }

        return $this->inject->apply($base, $last);
    }

    /**
     * @return array<mixed>
     */
    private function records(DecodedResponse $response): array
    {
        $body = $response->body();

        if (!array_dot_exists($body, $this->recordPath)) {
            return [];
        }

        /** @var array<mixed>|scalar|null $records */
        $records = array_dot_get($body, $this->recordPath);

        return is_array($records) ? $records : [];
    }
}
