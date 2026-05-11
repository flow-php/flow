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

    protected function setUp(): void
    {
        $this->originalServer = $_SERVER;
        $this->originalGet = $_GET;
        $this->originalPost = $_POST;
        $this->originalCookie = $_COOKIE;

        $_GET = [];
        $_POST = [];
        $_COOKIE = [];

        foreach ($_SERVER as $key => $_) {
            if (\is_string($key) && \str_starts_with($key, 'HTTP_')) {
                unset($_SERVER[$key]);
            }
        }
    }

    protected function tearDown(): void
    {
        $_SERVER = $this->originalServer;
        $_GET = $this->originalGet;
        $_POST = $this->originalPost;
        $_COOKIE = $this->originalCookie;
    }

    public function test_get_from_cookie(): void
    {
        $_COOKIE['session_id'] = 'abc123';

        $carrier = new SuperglobalCarrier();

        static::assertSame('abc123', $carrier->get('session_id'));
    }

    public function test_get_from_get(): void
    {
        $_GET['page'] = '1';

        $carrier = new SuperglobalCarrier();

        static::assertSame('1', $carrier->get('page'));
    }

    public function test_get_from_post(): void
    {
        $_POST['username'] = 'john';

        $carrier = new SuperglobalCarrier();

        static::assertSame('john', $carrier->get('username'));
    }

    public function test_get_from_server_http_header(): void
    {
        $_SERVER['HTTP_TRACEPARENT'] = '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01';

        $carrier = new SuperglobalCarrier();

        static::assertSame('00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01', $carrier->get('traceparent'));
    }

    public function test_get_is_case_insensitive_for_get(): void
    {
        $_GET['MyKey'] = 'value';

        $carrier = new SuperglobalCarrier();

        static::assertSame('value', $carrier->get('mykey'));
        static::assertSame('value', $carrier->get('MYKEY'));
        static::assertSame('value', $carrier->get('MyKey'));
    }

    public function test_get_is_case_insensitive_for_http_headers(): void
    {
        $_SERVER['HTTP_X_CUSTOM_HEADER'] = 'custom-value';

        $carrier = new SuperglobalCarrier();

        static::assertSame('custom-value', $carrier->get('x-custom-header'));
        static::assertSame('custom-value', $carrier->get('X-CUSTOM-HEADER'));
        static::assertSame('custom-value', $carrier->get('X-Custom-Header'));
    }

    public function test_get_priority_get_over_post(): void
    {
        $_GET['key'] = 'from-get';
        $_POST['key'] = 'from-post';

        $carrier = new SuperglobalCarrier();

        static::assertSame('from-get', $carrier->get('key'));
    }

    public function test_get_priority_post_over_cookie(): void
    {
        $_POST['key'] = 'from-post';
        $_COOKIE['key'] = 'from-cookie';

        $carrier = new SuperglobalCarrier();

        static::assertSame('from-post', $carrier->get('key'));
    }

    public function test_get_priority_server_over_get(): void
    {
        $_SERVER['HTTP_TRACEPARENT'] = 'from-server';
        $_GET['traceparent'] = 'from-get';

        $carrier = new SuperglobalCarrier();

        static::assertSame('from-server', $carrier->get('traceparent'));
    }

    public function test_get_returns_null_for_missing_key(): void
    {
        $carrier = new SuperglobalCarrier();

        static::assertNull($carrier->get('nonexistent'));
    }

    public function test_get_skips_non_string_values(): void
    {
        $_GET['array_value'] = ['not', 'a', 'string'];
        $_POST['array_value'] = 'string-value';

        $carrier = new SuperglobalCarrier();

        static::assertSame('string-value', $carrier->get('array_value'));
    }

    public function test_set_throws_runtime_exception(): void
    {
        $carrier = new SuperglobalCarrier();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('SuperglobalCarrier is read-only');

        $carrier->set('key', 'value');
    }

    public function test_unwrap_returns_all_data(): void
    {
        $_SERVER['HTTP_TRACEPARENT'] = '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01';
        $_GET['page'] = '1';
        $_POST['username'] = 'john';
        $_COOKIE['session_id'] = 'abc123';

        $carrier = new SuperglobalCarrier();

        $data = $carrier->unwrap();

        static::assertSame('00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01', $data['traceparent']);
        static::assertSame('1', $data['page']);
        static::assertSame('john', $data['username']);
        static::assertSame('abc123', $data['session_id']);
    }

    public function test_unwrap_returns_empty_array_when_no_data(): void
    {
        $carrier = new SuperglobalCarrier();

        $data = $carrier->unwrap();

        static::assertSame([], $data);
    }
}
