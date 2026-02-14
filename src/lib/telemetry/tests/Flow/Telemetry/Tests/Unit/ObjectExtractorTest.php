<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\ObjectExtractor;
use PHPUnit\Framework\TestCase;

final class ObjectExtractorTest extends TestCase
{
    public function test_all_uppercase_class() : void
    {
        self::assertSame('abc', ObjectExtractor::shortName(new ABC()));
    }

    public function test_anonymous_class() : void
    {
        $anonymous = new class {};

        self::assertStringContainsString('class@anonymous', ObjectExtractor::shortName($anonymous));
    }

    public function test_consecutive_uppercase_letters() : void
    {
        self::assertSame('http_client', ObjectExtractor::shortName(new HTTPClient()));
    }

    public function test_namespaced_class_extracts_short_name() : void
    {
        self::assertSame('object_extractor_test', ObjectExtractor::shortName($this));
    }

    public function test_pascal_case_to_snake_case() : void
    {
        self::assertSame('object_extractor', ObjectExtractor::shortName(new ObjectExtractor()));
    }

    public function test_simple_class_name() : void
    {
        self::assertSame('std_class', ObjectExtractor::shortName(new \stdClass()));
    }

    public function test_single_letter_class() : void
    {
        self::assertSame('a', ObjectExtractor::shortName(new A()));
    }
}

class HTTPClient
{
}

class A
{
}

class ABC
{
}
