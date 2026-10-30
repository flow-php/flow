<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\LinkHeaderParser;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PaginationStrategy;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\RequestOption;
use Psr\Http\Message\RequestInterface;

final class LinkHeader implements PaginationStrategy
{
    private LinkHeaderParser $parser;

    public function __construct(
        private readonly string $rel = 'next',
    ) {
        $this->parser = new LinkHeaderParser();
    }

    public function initialRequest(RequestInterface $base): RequestInterface
    {
        return $base;
    }

    public function nextRequest(RequestInterface $base, DecodedResponse $previous, PageState $state): ?RequestInterface
    {
        $url = $this->parser->next($previous->header('Link'), $this->rel);

        if ($url === null) {
            return null;
        }

        return RequestOption::replaceUri()->apply($base, $url);
    }
}
