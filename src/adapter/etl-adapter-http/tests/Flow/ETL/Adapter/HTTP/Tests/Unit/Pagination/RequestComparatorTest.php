<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Http\Pagination\RequestComparator;
use Flow\ETL\Tests\FlowTestCase;
use Nyholm\Psr7\Factory\Psr17Factory;

final class RequestComparatorTest extends FlowTestCase
{
    public function test_differs_when_body_differs(): void
    {
        $factory = new Psr17Factory();
        $left = $factory
            ->createRequest('POST', 'https://api.example.com/search')
            ->withBody($factory->createStream('{"cursor":"a"}'));
        $right = $factory
            ->createRequest('POST', 'https://api.example.com/search')
            ->withBody($factory->createStream('{"cursor":"b"}'));

        static::assertFalse((new RequestComparator())->equals($left, $right));
    }

    public function test_differs_when_uri_differs(): void
    {
        $factory = new Psr17Factory();

        static::assertFalse((new RequestComparator())->equals(
            $factory->createRequest('GET', 'https://api.example.com/items?page=1'),
            $factory->createRequest('GET', 'https://api.example.com/items?page=2'),
        ));
    }

    public function test_equal_when_method_uri_and_body_match(): void
    {
        $factory = new Psr17Factory();

        static::assertTrue((new RequestComparator())->equals(
            $factory->createRequest('GET', 'https://api.example.com/items?page=1'),
            $factory->createRequest('GET', 'https://api.example.com/items?page=1'),
        ));
    }
}
