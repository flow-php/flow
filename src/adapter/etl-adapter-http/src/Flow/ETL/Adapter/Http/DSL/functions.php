<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\{Adapter\Http\DynamicExtractor\NextRequestFactory,
    Attribute\DocumentationDSL,
    Attribute\Module,
    Attribute\Type};
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;

#[DocumentationDSL(module: Module::HTTP, type: Type::EXTRACTOR)]
function from_dynamic_http_requests(
    ClientInterface $client,
    NextRequestFactory $requestFactory,
) : PsrHttpClientDynamicExtractor {
    return new PsrHttpClientDynamicExtractor(
        $client,
        $requestFactory,
    );
}

/**
 * @param iterable<RequestInterface> $requests
 */
#[DocumentationDSL(module: Module::HTTP, type: Type::EXTRACTOR)]
function from_static_http_requests(
    ClientInterface $client,
    iterable $requests,
) : PsrHttpClientStaticExtractor {
    return new PsrHttpClientStaticExtractor(
        $client,
        $requests,
    );
}
