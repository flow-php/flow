<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit;

use Flow\ETL\Adapter\Http\HttpEncoder;
use Flow\ETL\Adapter\Http\HttpExchange;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use Psr\Http\Message\ResponseInterface;

use function json_encode;

final class HttpEncoderTest extends FlowTestCase
{
    public static function structured_bodies(): Generator
    {
        yield 'JSON decodes to a navigable map' => [
            new Response(headers: ['Content-Type' => 'application/vnd.api+json'], body: json_encode([
                'meta' => ['next' => 'abc'],
                'data' => [1, 2],
            ], JSON_THROW_ON_ERROR)),
            ['meta' => ['next' => 'abc'], 'data' => [1, 2]],
        ];

        yield 'XML decodes to a navigable map' => [
            new Response(headers: [
                'Content-Type' => 'text/xml; charset=UTF-8',
            ], body: '<result><IdList><Id>1</Id><Id>2</Id></IdList></result>'),
            ['result' => ['IdList' => ['Id' => [['@value' => '1'], ['@value' => '2']]]]],
        ];

        yield 'a text body has no structure to navigate' => [
            new Response(headers: ['Content-Type' => 'text/plain'], body: 'just text'),
            [],
        ];

        yield 'an empty body has no structure to navigate' => [
            new Response(headers: ['Content-Type' => 'application/json']),
            [],
        ];
    }

    public static function bodies(): Generator
    {
        yield 'JSON body stays raw text in the row' => [
            new Response(headers: ['Content-Type' => 'application/vnd.api+json'], body: json_encode([
                'meta' => ['next' => 'abc'],
                'data' => [1, 2],
            ], JSON_THROW_ON_ERROR)),
            '{"meta":{"next":"abc"},"data":[1,2]}',
        ];

        yield 'XML body stays raw text in the row' => [
            new Response(headers: [
                'Content-Type' => 'text/xml; charset=UTF-8',
            ], body: '<result><IdList><Id>1</Id><Id>2</Id></IdList></result>'),
            '<result><IdList><Id>1</Id><Id>2</Id></IdList></result>',
        ];

        yield 'text body stays raw text' => [
            new Response(headers: ['Content-Type' => 'text/plain'], body: 'just text'),
            'just text',
        ];

        yield 'empty body is null' => [
            new Response(headers: ['Content-Type' => 'application/json']),
            null,
        ];
    }

    #[DataProvider('bodies')]
    public function test_a_row_carries_the_response_body_as_raw_text(
        ResponseInterface $response,
        ?string $expected,
    ): void {
        $exchange = new HttpExchange((new Psr17Factory())->createRequest('GET', 'https://api.example.com'), $response);

        static::assertSame($expected, (new HttpEncoder())->decode([$exchange])[0]->values['response_body']);
    }

    public function test_decodes_full_row_values(): void
    {
        $request = (new Psr17Factory())->createRequest('GET', 'https://api.github.com/orgs/flow-php');
        $response = new Response(
            status: 201,
            headers: ['Server' => 'GitHub.com', 'Content-Type' => 'application/json'],
            body: json_encode(['id' => 1], JSON_THROW_ON_ERROR),
        );

        $values = (new HttpEncoder())->decode([new HttpExchange($request, $response)])[0]->values;

        static::assertSame('{"id":1}', $values['response_body']);
        static::assertSame(
            ['Server' => ['GitHub.com'], 'Content-Type' => ['application/json']],
            $values['response_headers'],
        );
        static::assertSame(201, $values['response_status_code']);
        static::assertSame('1.1', $values['response_protocol_version']);
        static::assertSame('Created', $values['response_reason_phrase']);
        static::assertSame('https://api.github.com/orgs/flow-php', $values['request_uri']);
        static::assertSame('GET', $values['request_method']);
        static::assertSame('1.1', $values['request_protocol_version']);
    }

    public function test_encode_is_unsupported(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('read-only');

        (new HttpEncoder())->encode([]);
    }

    /**
     * @param array<mixed> $expected
     */
    #[DataProvider('structured_bodies')]
    public function test_pagination_reads_a_structured_body(ResponseInterface $response, array $expected): void
    {
        static::assertSame($expected, (new HttpEncoder())->structuredBody($response));
    }

    public function test_a_row_carries_an_undecodable_body_as_text(): void
    {
        static::assertSame(
            'not json {',
            (new HttpEncoder())->decode([new HttpExchange(
                (new Psr17Factory())->createRequest('GET', 'https://api.example.com'),
                new Response(503, ['Content-Type' => 'application/json'], 'not json {'),
            )])[0]->values['response_body'],
        );
    }

    public function test_invalid_json_body_throws_runtime_exception(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('status 503');

        (new HttpEncoder())->structuredBody(new Response(503, ['Content-Type' => 'application/json'], 'not json {'));
    }
}
