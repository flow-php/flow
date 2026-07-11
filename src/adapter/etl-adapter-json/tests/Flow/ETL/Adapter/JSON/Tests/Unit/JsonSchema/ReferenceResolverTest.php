<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\JsonSchema;

use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnresolvableReferenceException;
use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnsupportedKeywordException;
use Flow\ETL\Adapter\JSON\JsonSchema\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;

use function file_get_contents;
use function Flow\Filesystem\DSL\path;
use function json_decode;
use function json_encode;

final class ReferenceResolverTest extends FlowTestCase
{
    public function test_load_from_path(): void
    {
        $loaded = (new ReferenceResolver())->load(path(__DIR__ . '/../../Fixtures/json-schema/address.json'));

        static::assertSame('object', $loaded->schema['type']);
        static::assertSame($loaded->schema, $loaded->document);
        static::assertStringEndsWith('Fixtures/json-schema/address.json', $loaded->baseUri);
    }

    public function test_load_honors_document_id_as_base_uri(): void
    {
        $client = new Client();
        $client->addResponse(
            new Response(
                200,
                [],
                (string) json_encode([
                    '$id' => 'https://example.com/schemas/root.json',
                    'type' => 'object',
                ]),
            ),
        );

        $loaded = (new ReferenceResolver($client, new Psr17Factory()))->load('https://cdn.example.com/root.json');

        static::assertSame('https://example.com/schemas/root.json', $loaded->baseUri);
    }

    public function test_resolve_anchor_reference_is_not_supported(): void
    {
        $this->expectException(UnsupportedKeywordException::class);
        $this->expectExceptionMessage('JSON Schema keyword "$anchor"');

        (new ReferenceResolver())->resolve('#address', '', ['$defs' => []]);
    }

    public function test_resolve_deep_pointer_with_escaped_segments(): void
    {
        $resolved = (new ReferenceResolver())->resolve('#/$defs/paths/~1users~1id/schema', '', [
            '$defs' => ['paths' => ['/users/id' => ['schema' => ['type' => 'string']]]],
        ]);

        static::assertSame(['type' => 'string'], $resolved->schema);
    }

    public function test_resolve_document_cache_fetches_each_remote_document_once(): void
    {
        $client = new Client();
        $client->addResponse(
            new Response(
                200,
                [],
                (string) json_encode([
                    '$defs' => [
                        'a' => ['type' => 'string'],
                        'b' => ['type' => 'integer'],
                    ],
                ]),
            ),
        );

        $resolver = new ReferenceResolver($client, new Psr17Factory());

        $resolver->resolve('https://example.com/common.json#/$defs/a', '', []);
        $resolver->resolve('https://example.com/common.json#/$defs/b', '', []);

        static::assertCount(1, $client->getRequests());
    }

    public function test_resolve_internal_pointer_via_defs(): void
    {
        $document = ['$defs' => ['money' => ['type' => 'object']]];

        $resolved = (new ReferenceResolver())->resolve('#/$defs/money', 'file://schemas/root.json', $document);

        static::assertSame(['type' => 'object'], $resolved->schema);
        static::assertSame($document, $resolved->document);
        static::assertSame('file://schemas/root.json', $resolved->baseUri);
        static::assertSame('file://schemas/root.json#/$defs/money', $resolved->identity);
    }

    public function test_resolve_internal_pointer_via_legacy_definitions(): void
    {
        $resolved = (new ReferenceResolver())->resolve('#/definitions/status', '', [
            'definitions' => ['status' => ['type' => 'string']],
        ]);

        static::assertSame(['type' => 'string'], $resolved->schema);
    }

    public function test_resolve_pointer_to_missing_segment_throws_exception(): void
    {
        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage('pointer segment "missing" does not exist');

        (new ReferenceResolver())->resolve('#/$defs/missing', '', ['$defs' => []]);
    }

    public function test_resolve_pointer_to_scalar_throws_exception(): void
    {
        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage('reference target is not a schema object');

        (new ReferenceResolver())->resolve('#/$defs/name', '', ['$defs' => ['name' => 'scalar']]);
    }

    public function test_resolve_relative_file_reference(): void
    {
        $resolver = new ReferenceResolver();
        $loaded = $resolver->load(path(__DIR__ . '/../../Fixtures/json-schema/person.json'));

        $resolved = $resolver->resolve('address.json', $loaded->baseUri, $loaded->document);

        static::assertSame(
            json_decode((string) file_get_contents(__DIR__ . '/../../Fixtures/json-schema/address.json'), true),
            $resolved->schema,
        );
        static::assertStringEndsWith('Fixtures/json-schema/address.json', $resolved->baseUri);
    }

    public function test_resolve_relative_file_reference_from_subdirectory_with_pointer(): void
    {
        $resolver = new ReferenceResolver();
        $loaded = $resolver->load(path(__DIR__ . '/../../Fixtures/json-schema/person.json'));

        $resolved = $resolver->resolve('types/geo.json#/$defs/coordinates', $loaded->baseUri, $loaded->document);

        static::assertSame(
            [
                'type' => 'object',
                'required' => ['lat', 'lon'],
                'properties' => [
                    'lat' => ['type' => 'number'],
                    'lon' => ['type' => 'number'],
                ],
            ],
            $resolved->schema,
        );
    }

    public function test_resolve_relative_reference_without_base_uri_throws_exception(): void
    {
        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage('relative reference cannot be resolved without a base URI');

        (new ReferenceResolver())->resolve('address.json#/properties/street', '', []);
    }

    public function test_resolve_remote_reference(): void
    {
        $client = new Client();
        $client->addResponse(
            new Response(
                200,
                [],
                (string) json_encode([
                    '$defs' => ['status' => ['type' => 'string']],
                ]),
            ),
        );

        $resolved = (new ReferenceResolver($client, new Psr17Factory()))->resolve(
            'https://example.com/common.json#/$defs/status',
            '',
            [],
        );

        static::assertSame(['type' => 'string'], $resolved->schema);
        static::assertSame('https://example.com/common.json#/$defs/status', $resolved->identity);
        static::assertSame('https://example.com/common.json', (string) $client->getRequests()[0]->getUri());
    }

    public function test_resolve_remote_reference_relative_to_remote_base_uri(): void
    {
        $client = new Client();
        $client->addResponse(
            new Response(
                200,
                [],
                (string) json_encode([
                    '$defs' => ['status' => ['type' => 'string']],
                ]),
            ),
        );

        $resolved = (new ReferenceResolver($client, new Psr17Factory()))->resolve(
            'common.json#/$defs/status',
            'https://example.com/schemas/root.json',
            [],
        );

        static::assertSame(['type' => 'string'], $resolved->schema);
        static::assertSame('https://example.com/schemas/common.json', (string) $client->getRequests()[0]->getUri());
    }

    public function test_resolve_remote_reference_returning_invalid_json_throws_exception(): void
    {
        $client = new Client();
        $client->addResponse(new Response(200, [], '{invalid'));

        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage('is not a valid JSON object');

        (new ReferenceResolver($client, new Psr17Factory()))->resolve(
            'https://example.com/common.json#/$defs/status',
            '',
            [],
        );
    }

    public function test_resolve_remote_reference_returning_non_200_status_throws_exception(): void
    {
        $client = new Client();
        $client->addResponse(new Response(404, [], ''));

        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage('returned status code 404');

        (new ReferenceResolver($client, new Psr17Factory()))->resolve(
            'https://example.com/common.json#/$defs/status',
            '',
            [],
        );
    }

    public function test_resolve_remote_reference_without_client_throws_exception(): void
    {
        $this->expectException(UnresolvableReferenceException::class);
        $this->expectExceptionMessage(
            'resolving remote references requires a PSR-18 http client and a PSR-17 request factory',
        );

        (new ReferenceResolver())->resolve('https://example.com/common.json#/$defs/status', '', []);
    }

    public function test_resolve_whole_document_reference_without_pointer(): void
    {
        $client = new Client();
        $client->addResponse(new Response(200, [], (string) json_encode(['type' => 'object'])));

        $resolved = (new ReferenceResolver($client, new Psr17Factory()))->resolve(
            'https://example.com/common.json',
            '',
            [],
        );

        static::assertSame(['type' => 'object'], $resolved->schema);
    }
}
