<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\AuthorizationFactory;

use Flow\Azure\SDK\AuthorizationFactory\SharedKeyFactory;
use Http\Discovery\Psr17FactoryDiscovery;
use PHPUnit\Framework\TestCase;

final class SharedKeyFactoryTest extends TestCase
{
    public function test_for_returns_string_starting_with_shared_key_prefix_and_account(): void
    {
        $factory = new SharedKeyFactory('testaccount', base64_encode('secret-key-value'));

        $request = Psr17FactoryDiscovery::findRequestFactory()->createRequest(
            'GET',
            'https://testaccount.blob.core.windows.net/container/blob',
        );

        static::assertStringStartsWith('SharedKey testaccount:', $factory->for($request));
    }

    public function test_for_includes_base64_encoded_signature(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));

        $request = Psr17FactoryDiscovery::findRequestFactory()->createRequest(
            'GET',
            'https://account.blob.core.windows.net/container',
        );

        $authorization = $factory->for($request);

        $signature = substr($authorization, strlen('SharedKey account:'));

        static::assertSame(base64_encode((string) base64_decode($signature, true)), $signature);
    }

    public function test_for_is_deterministic_for_same_request(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));

        $request = Psr17FactoryDiscovery::findRequestFactory()->createRequest(
            'GET',
            'https://account.blob.core.windows.net/container/blob',
        );

        static::assertSame($factory->for($request), $factory->for($request));
    }

    public function test_for_changes_signature_when_url_differs(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $requestFactory = Psr17FactoryDiscovery::findRequestFactory();

        $authA = $factory->for($requestFactory->createRequest('GET', 'https://account.blob.core.windows.net/a'));
        $authB = $factory->for($requestFactory->createRequest('GET', 'https://account.blob.core.windows.net/b'));

        static::assertNotSame($authA, $authB);
    }

    public function test_for_changes_signature_when_http_method_differs(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $requestFactory = Psr17FactoryDiscovery::findRequestFactory();
        $url = 'https://account.blob.core.windows.net/container/blob';

        $authGet = $factory->for($requestFactory->createRequest('GET', $url));
        $authPut = $factory->for($requestFactory->createRequest('PUT', $url));

        static::assertNotSame($authGet, $authPut);
    }

    public function test_for_changes_signature_when_x_ms_header_differs(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $requestFactory = Psr17FactoryDiscovery::findRequestFactory();
        $url = 'https://account.blob.core.windows.net/container/blob';

        $authA = $factory->for($requestFactory->createRequest('GET', $url)->withHeader('x-ms-version', '2024-08-04'));
        $authB = $factory->for($requestFactory->createRequest('GET', $url)->withHeader('x-ms-version', '2020-01-01'));

        static::assertNotSame($authA, $authB);
    }

    public function test_for_changes_signature_when_query_parameters_differ(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $requestFactory = Psr17FactoryDiscovery::findRequestFactory();

        $authA = $factory->for($requestFactory->createRequest(
            'GET',
            'https://account.blob.core.windows.net/container?comp=list',
        ));
        $authB = $factory->for($requestFactory->createRequest(
            'GET',
            'https://account.blob.core.windows.net/container?comp=metadata',
        ));

        static::assertNotSame($authA, $authB);
    }

    public function test_for_canonicalizes_multi_value_headers_via_comma(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $requestFactory = Psr17FactoryDiscovery::findRequestFactory();
        $url = 'https://account.blob.core.windows.net/container/blob';

        $multi = $requestFactory->createRequest('GET', $url)->withHeader('x-ms-meta-tags', ['red', 'green']);

        $merged = $requestFactory->createRequest('GET', $url)->withHeader('x-ms-meta-tags', 'red,green');

        static::assertSame($factory->for($multi), $factory->for($merged));
    }

    public function test_for_handles_url_without_path(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $request = Psr17FactoryDiscovery::findRequestFactory()->createRequest(
            'GET',
            'https://account.blob.core.windows.net',
        );

        static::assertStringStartsWith('SharedKey account:', $factory->for($request));
    }

    public function test_for_includes_content_headers_in_string_to_sign(): void
    {
        $factory = new SharedKeyFactory('account', base64_encode('key'));
        $requestFactory = Psr17FactoryDiscovery::findRequestFactory();
        $url = 'https://account.blob.core.windows.net/container/blob';

        $withContentType = $requestFactory->createRequest('PUT', $url)->withHeader(
            'content-type',
            'application/octet-stream',
        );

        $withoutContentType = $requestFactory->createRequest('PUT', $url);

        static::assertNotSame($factory->for($withContentType), $factory->for($withoutContentType));
    }
}
