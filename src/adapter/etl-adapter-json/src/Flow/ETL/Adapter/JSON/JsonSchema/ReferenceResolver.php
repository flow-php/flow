<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\JsonSchema;

use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnresolvableReferenceException;
use Flow\ETL\Adapter\JSON\JsonSchema\Exception\UnsupportedKeywordException;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;

use function array_key_exists;
use function array_map;
use function array_pop;
use function explode;
use function implode;
use function is_array;
use function is_string;
use function json_decode;
use function preg_match;
use function rawurldecode;
use function rtrim;
use function sprintf;
use function str_contains;
use function str_replace;
use function str_starts_with;
use function strrpos;
use function substr;

final class ReferenceResolver
{
    /**
     * @var array<string, array<string, mixed>>
     */
    private array $documents = [];

    public function __construct(
        private readonly ?ClientInterface $client = null,
        private readonly ?RequestFactoryInterface $requestFactory = null,
        private readonly Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {}

    /**
     * @throws UnresolvableReferenceException
     */
    public function load(string|Path $source): ResolvedReference
    {
        $uri = $source instanceof Path ? $source->uri() : $source;
        $document = $this->document($uri, $source instanceof Path ? $source : null);

        $baseUri = $uri;

        if (array_key_exists('$id', $document) && is_string($document['$id'])) {
            $baseUri = $this->absolutize($document['$id'], $uri);
        }

        return new ResolvedReference($document, $document, $baseUri, $uri);
    }

    /**
     * @param array<string, mixed> $rootDocument document in which the reference was found
     *
     * @throws UnresolvableReferenceException|UnsupportedKeywordException
     */
    public function resolve(string $ref, string $baseUri, array $rootDocument): ResolvedReference
    {
        $target = $ref;
        $pointer = '';

        if (str_contains($ref, '#')) {
            [$target, $pointer] = explode('#', $ref, 2);
        }

        if ($pointer !== '' && !str_starts_with($pointer, '/')) {
            throw new UnsupportedKeywordException('$anchor', $ref);
        }

        if ($target === '') {
            return new ResolvedReference(
                $this->pointer($rootDocument, $pointer, $ref),
                $rootDocument,
                $baseUri,
                $baseUri . '#' . $pointer,
            );
        }

        $absoluteUri = $this->absolutize($target, $baseUri);
        $document = $this->document($absoluteUri, null, $ref);

        $documentBaseUri = $absoluteUri;

        if (array_key_exists('$id', $document) && is_string($document['$id'])) {
            $documentBaseUri = $this->absolutize($document['$id'], $absoluteUri);
        }

        return new ResolvedReference(
            $this->pointer($document, $pointer, $ref),
            $document,
            $documentBaseUri,
            $absoluteUri . '#' . $pointer,
        );
    }

    /**
     * @throws UnresolvableReferenceException
     */
    private function absolutize(string $target, string $baseUri): string
    {
        if (preg_match('/^[a-z][a-z0-9+.\-]*:\/\//i', $target)) {
            return $target;
        }

        if ($baseUri === '') {
            throw new UnresolvableReferenceException(
                $target,
                'relative reference cannot be resolved without a base URI, load the schema from a Path or provide an absolute reference',
            );
        }

        if (str_starts_with($target, '/') && str_starts_with($baseUri, 'file://')) {
            return Path::from($target)->uri();
        }

        $matches = [];

        if (!preg_match('/^([a-z][a-z0-9+.\-]*:\/\/[^\/]*)(\/.*)?$/i', $baseUri, $matches)) {
            throw new UnresolvableReferenceException($target, sprintf('base URI "%s" is not absolute', $baseUri));
        }

        $root = $matches[1];
        $basePath = $matches[2] ?? '/';

        if (str_starts_with($target, '/')) {
            return $root . $target;
        }

        $directory = substr($basePath, 0, (int) strrpos($basePath, '/'));

        return $root . $this->normalizePath($directory . '/' . $target);
    }

    /**
     * @return array<string, mixed>
     *
     * @throws UnresolvableReferenceException
     */
    private function document(string $uri, ?Path $path, ?string $ref = null): array
    {
        if (array_key_exists($uri, $this->documents)) {
            return $this->documents[$uri];
        }

        if (str_starts_with($uri, 'http://') || str_starts_with($uri, 'https://')) {
            $content = $this->fetch($uri, $ref ?? $uri);
        } else {
            $path ??= Path::from($uri);

            if (!$this->filesystem->supports($path)) {
                throw new UnresolvableReferenceException($ref ?? $uri, sprintf(
                    'filesystem %s serves "%s://" paths, given: "%s"',
                    $this->filesystem::class,
                    $this->filesystem->mount()->protocol,
                    $path->uri(),
                ));
            }

            $content = $this->filesystem->readFrom($path)->content();
        }

        // @mago-ignore analysis:mixed-assignment
        $document = json_decode($content, true);

        if (!is_array($document)) {
            throw new UnresolvableReferenceException($ref ?? $uri, sprintf(
                'document "%s" is not a valid JSON object',
                $uri,
            ));
        }

        /** @var array<string, mixed> $document */
        $this->documents[$uri] = $document;

        return $document;
    }

    /**
     * @throws UnresolvableReferenceException
     */
    private function fetch(string $uri, string $ref): string
    {
        if ($this->client === null || $this->requestFactory === null) {
            throw new UnresolvableReferenceException(
                $ref,
                'resolving remote references requires a PSR-18 http client and a PSR-17 request factory',
            );
        }

        $response = $this->client->sendRequest($this->requestFactory->createRequest('GET', $uri));

        if ($response->getStatusCode() !== 200) {
            throw new UnresolvableReferenceException($ref, sprintf(
                'fetching "%s" returned status code %d',
                $uri,
                $response->getStatusCode(),
            ));
        }

        return (string) $response->getBody();
    }

    private function normalizePath(string $path): string
    {
        /** @var list<string> $segments */
        $segments = [];

        foreach (explode('/', $path) as $segment) {
            if ($segment === '' || $segment === '.') {
                continue;
            }

            if ($segment === '..') {
                array_pop($segments);

                continue;
            }

            $segments[] = $segment;
        }

        return '/' . implode('/', $segments);
    }

    /**
     * @param array<string, mixed> $document
     *
     * @return array<string, mixed>
     *
     * @throws UnresolvableReferenceException
     */
    private function pointer(array $document, string $pointer, string $ref): array
    {
        if ($pointer === '') {
            return $document;
        }

        $current = $document;

        foreach (array_map(
            static fn(string $segment): string => str_replace(['~1', '~0'], ['/', '~'], rawurldecode($segment)),
            explode('/', rtrim(substr($pointer, 1), '/')),
        ) as $segment) {
            if (!is_array($current) || !array_key_exists($segment, $current)) {
                throw new UnresolvableReferenceException($ref, sprintf(
                    'pointer segment "%s" does not exist',
                    $segment,
                ));
            }

            // @mago-ignore analysis:mixed-assignment
            $current = $current[$segment];
        }

        if (!is_array($current)) {
            throw new UnresolvableReferenceException($ref, 'reference target is not a schema object');
        }

        /** @var array<string, mixed> $current */
        return $current;
    }
}
