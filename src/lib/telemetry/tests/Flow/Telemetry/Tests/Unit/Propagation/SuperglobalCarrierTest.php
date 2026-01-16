<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Propagation;

use Flow\Telemetry\Exception\RuntimeException;
use Flow\Telemetry\Propagation\SuperglobalCarrier;
use PHPUnit\Framework\TestCase;

final class SuperglobalCarrierTest extends TestCase
{
    /**
     * @var array<string, mixed>
     */
    private array $originalCookie;

    /**
     * @var array<string, mixed>
     */
    private array $originalGet;

    /**
     * @var array<string, mixed>
     */
    private array $originalPost;

    /**
     * @var array<string, mixed>
     */
    private array $originalServer;

    protected function setUp() : void
    {
        $this->originalServer = $_SERVER;
        $this->originalGet = $_GET;
        $this->originalPost = $_POST;
        $this->originalCookie = $_COOKIE;

        $_GET = [];
        $_POST = [];
        $_COOKIE = [];
    }

    protected function tearDown() : void
    {
        $_SERVER = $this->originalServer;
        $_GET = $this->originalGet;
        $_POST = $this->originalPost;
        $_COOKIE = $this->originalCookie;
    }

    public function test_get_from_cookie() : void
    {
        $_COOKIE['session_id'] = 'abc123';

        $carrier = new SuperglobalCarrier();

        self::assertSame('abc123', $carrier->get('session_id'));
    }

    public function test_get_from_get() : void
    {
        $_GET['page'] = '1';

        $carrier = new SuperglobalCarrier();

        self::assertSame('1', $carrier->get('page'));
    }

    public function test_get_from_post() : void
    {
        $_POST['username'] = 'john';

        $carrier = new SuperglobalCarrier();

        self::assertSame('john', $carrier->get('username'));
    }

    public function test_get_from_server_http_header() : void
    {
        $_SERVER['HTTP_TRACEPARENT'] = '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01';

        $carrier = new SuperglobalCarrier();

        self::assertSame(
            '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
            $carrier->get('traceparent'),
        );
    }

    public function test_get_is_case_insensitive_for_get() : void
    {
        $_GET['MyKey'] = 'value';

        $carrier = new SuperglobalCarrier();

        self::assertSame('value', $carrier->get('mykey'));
        self::assertSame('value', $carrier->get('MYKEY'));
        self::assertSame('value', $carrier->get('MyKey'));
    }

    public function test_get_is_case_insensitive_for_http_headers() : void
    {
        $_SERVER['HTTP_X_CUSTOM_HEADER'] = 'custom-value';

        $carrier = new SuperglobalCarrier();

        self::assertSame('custom-value', $carrier->get('x-custom-header'));
        self::assertSame('custom-value', $carrier->get('X-CUSTOM-HEADER'));
        self::assertSame('custom-value', $carrier->get('X-Custom-Header'));
    }

    public function test_get_priority_get_over_post() : void
    {
        $_GET['key'] = 'from-get';
        $_POST['key'] = 'from-post';

        $carrier = new SuperglobalCarrier();

        self::assertSame('from-get', $carrier->get('key'));
    }

    public function test_get_priority_post_over_cookie() : void
    {
        $_POST['key'] = 'from-post';
        $_COOKIE['key'] = 'from-cookie';

        $carrier = new SuperglobalCarrier();

        self::assertSame('from-post', $carrier->get('key'));
    }

    public function test_get_priority_server_over_get() : void
    {
        $_SERVER['HTTP_TRACEPARENT'] = 'from-server';
        $_GET['traceparent'] = 'from-get';

        $carrier = new SuperglobalCarrier();

        self::assertSame('from-server', $carrier->get('traceparent'));
    }

    public function test_get_returns_null_for_missing_key() : void
    {
        $carrier = new SuperglobalCarrier();

        self::assertNull($carrier->get('nonexistent'));
    }

    public function test_get_skips_non_string_values() : void
    {
        $_GET['array_value'] = ['not', 'a', 'string'];
        $_POST['array_value'] = 'string-value';

        $carrier = new SuperglobalCarrier();

        self::assertSame('string-value', $carrier->get('array_value'));
    }

    public function test_keys_returns_combined_keys() : void
    {
        $_SERVER['HTTP_TRACEPARENT'] = 'value';
        $_SERVER['HTTP_TRACESTATE'] = 'value';
        $_GET['page'] = '1';
        $_POST['username'] = 'john';
        $_COOKIE['session'] = 'abc';

        $carrier = new SuperglobalCarrier();
        $keys = $carrier->keys();

        self::assertContains('traceparent', $keys);
        self::assertContains('tracestate', $keys);
        self::assertContains('page', $keys);
        self::assertContains('username', $keys);
        self::assertContains('session', $keys);
    }

    public function test_keys_returns_unique_values() : void
    {
        $_GET['key'] = 'from-get';
        $_POST['key'] = 'from-post';

        $carrier = new SuperglobalCarrier();
        $keys = $carrier->keys();

        $keyCount = \array_count_values($keys)['key'] ?? 0;
        self::assertSame(1, $keyCount);
    }

    public function test_set_throws_runtime_exception() : void
    {
        $carrier = new SuperglobalCarrier();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SuperglobalCarrier is read-only');

        $carrier->set('key', 'value');
    }
}
