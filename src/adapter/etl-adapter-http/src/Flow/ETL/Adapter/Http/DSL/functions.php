<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\ETL\Adapter\Http\IO\Extractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Paginator;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\RequestOption;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\AllOf;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\AnyOf;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\EmptyPath;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\EmptyResponseBody;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\FlagFalse;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\FlagTrue;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\MaxPages;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\MaxResults;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\NeverStop;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\PathMissing;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\StopWhen\TotalReached;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\CursorFromBody;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\LastRecordCursor;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\LinkHeader;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\NextUrlFromBody;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\OffsetLimit;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\PageNumber;
use Flow\ETL\Adapter\Http\IO\Extractor\PsrHttpClientDynamicExtractor;
use Flow\ETL\Adapter\Http\IO\Extractor\PsrHttpClientPaginatedExtractor;
use Flow\ETL\Adapter\Http\IO\Extractor\PsrHttpClientStaticExtractor;
use Flow\ETL\Values\Schema;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\StreamFactoryInterface;

#[DocumentationDSL(module: Module::HTTP, type: Type::EXTRACTOR)]
function from_dynamic_http_requests(
    ClientInterface $client,
    NextRequestFactory $requestFactory,
    ?Schema $schema = null,
): PsrHttpClientDynamicExtractor {
    $extractor = new PsrHttpClientDynamicExtractor($client, $requestFactory);

    if ($schema !== null) {
        $extractor->withSchema($schema);
    }

    return $extractor;
}

/**
 * @param iterable<RequestInterface> $requests
 */
#[DocumentationDSL(module: Module::HTTP, type: Type::EXTRACTOR)]
function from_static_http_requests(
    ClientInterface $client,
    iterable $requests,
    ?Schema $schema = null,
): PsrHttpClientStaticExtractor {
    $extractor = new PsrHttpClientStaticExtractor($client, $requests);

    if ($schema !== null) {
        $extractor->withSchema($schema);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::HTTP, type: Type::EXTRACTOR)]
function from_http_paginated(
    ClientInterface $client,
    RequestInterface $request,
    Paginator $paginator,
    ?Schema $schema = null,
): PsrHttpClientPaginatedExtractor {
    $extractor = new PsrHttpClientPaginatedExtractor($client, $request, $paginator);

    if ($schema !== null) {
        $extractor->withSchema($schema);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_pagination_page_number(
    RequestOption $inject,
    int $start_page = 1,
    ?int $page_size = null,
    ?RequestOption $size_option = null,
    bool $inject_on_first_request = true,
    ?string $records_path = null,
    bool $stop_on_client_error = true,
    ?StopWhen $stop_when = null,
): Paginator {
    $default = $records_path !== null ? new EmptyPath($records_path) : new EmptyResponseBody();

    return new Paginator(
        new PageNumber($inject, $start_page, $page_size, $size_option, $inject_on_first_request, $records_path),
        $stop_when === null ? $default : new AnyOf($default, $stop_when),
        $stop_on_client_error,
    );
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_pagination_offset(
    RequestOption $offset_option,
    RequestOption $limit_option,
    int $limit,
    int $start_offset = 0,
    ?string $total_path = null,
    bool $inject_on_first_request = true,
    bool $stop_on_client_error = true,
    ?StopWhen $stop_when = null,
): Paginator {
    $default = $total_path !== null ? new TotalReached($total_path) : new EmptyResponseBody();

    return new Paginator(
        new OffsetLimit($offset_option, $limit_option, $limit, $start_offset, $inject_on_first_request),
        $stop_when === null ? $default : new AnyOf($default, $stop_when),
        $stop_on_client_error,
    );
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_pagination_cursor(
    string $cursor_path,
    RequestOption $inject,
    bool $stop_on_client_error = true,
    ?StopWhen $stop_when = null,
): Paginator {
    $default = new PathMissing($cursor_path);

    return new Paginator(
        new CursorFromBody($cursor_path, $inject),
        $stop_when === null ? $default : new AnyOf($default, $stop_when),
        $stop_on_client_error,
    );
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_pagination_next_url(
    string $path,
    bool $stop_on_client_error = true,
    ?StopWhen $stop_when = null,
): Paginator {
    $default = new PathMissing($path);

    return new Paginator(
        new NextUrlFromBody($path),
        $stop_when === null ? $default : new AnyOf($default, $stop_when),
        $stop_on_client_error,
    );
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_pagination_link_header(
    string $rel = 'next',
    bool $stop_on_client_error = true,
    ?StopWhen $stop_when = null,
): Paginator {
    return new Paginator(new LinkHeader($rel), $stop_when ?? new NeverStop(), $stop_on_client_error);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_pagination_last_record_cursor(
    string $record_path,
    RequestOption $inject,
    bool $stop_on_client_error = true,
    ?StopWhen $stop_when = null,
): Paginator {
    $default = new EmptyResponseBody();

    return new Paginator(
        new LastRecordCursor($record_path, $inject),
        $stop_when === null ? $default : new AnyOf($default, $stop_when),
        $stop_on_client_error,
    );
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_request_option_query(string $name): RequestOption
{
    return RequestOption::queryParam($name);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_request_option_header(string $name): RequestOption
{
    return RequestOption::header($name);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_request_option_body(string $path, StreamFactoryInterface $stream_factory): RequestOption
{
    return RequestOption::bodyPath($path, $stream_factory);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_request_option_uri(): RequestOption
{
    return RequestOption::replaceUri();
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_path_missing(string $path): StopWhen
{
    return new PathMissing($path);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_empty_path(string $path): StopWhen
{
    return new EmptyPath($path);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_flag_false(string $path): StopWhen
{
    return new FlagFalse($path);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_flag_true(string $path): StopWhen
{
    return new FlagTrue($path);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_total_reached(string $total_path): StopWhen
{
    return new TotalReached($total_path);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_max_pages(int $pages): StopWhen
{
    return new MaxPages($pages);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_max_results(int $count): StopWhen
{
    return new MaxResults($count);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_any_of(StopWhen ...$conditions): StopWhen
{
    return new AnyOf(...$conditions);
}

#[DocumentationDSL(module: Module::HTTP, type: Type::HELPER)]
function http_stop_when_all_of(StopWhen ...$conditions): StopWhen
{
    return new AllOf(...$conditions);
}
