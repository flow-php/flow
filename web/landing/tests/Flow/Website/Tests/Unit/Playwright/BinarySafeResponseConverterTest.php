<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Playwright;

use Flow\Website\Playwright\BinarySafeResponseConverter;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Response;

use function file_get_contents;
use function Flow\Types\DSL\type_string;

final class BinarySafeResponseConverterTest extends TestCase
{
    public function test_a_binary_body_is_fulfilled_from_a_file_holding_the_decoded_bytes(): void
    {
        $wasm = "\0asm\1\0\0\0";
        $options = (new BinarySafeResponseConverter())->prepareFulfillOptions(new Response($wasm, 200, [
            'content-type' => 'application/wasm',
        ]));

        static::assertArrayNotHasKey('isBase64', $options);
        static::assertArrayNotHasKey('body', $options);
        static::assertArrayHasKey('path', $options);
        static::assertSame($wasm, file_get_contents(type_string()->assert($options['path'])));
    }

    public function test_a_text_body_is_left_exactly_as_upstream_produced_it(): void
    {
        $options = (new BinarySafeResponseConverter())->prepareFulfillOptions(new Response('body { color: red }', 200, [
            'content-type' => 'text/css',
        ]));

        static::assertSame('body { color: red }', type_string()->assert($options['body']));
        static::assertArrayNotHasKey('path', $options);
    }

    public function test_the_same_bytes_reuse_one_file(): void
    {
        $converter = new BinarySafeResponseConverter();
        $response = static fn(): Response => new Response("\0asm\1", 200, ['content-type' => 'application/wasm']);

        static::assertSame(
            type_string()->assert($converter->prepareFulfillOptions($response())['path']),
            type_string()->assert($converter->prepareFulfillOptions($response())['path']),
        );
    }
}
