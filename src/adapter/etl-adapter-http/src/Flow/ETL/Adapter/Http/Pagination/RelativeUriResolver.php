<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Psr\Http\Message\UriInterface;

use function array_key_exists;
use function array_pop;
use function explode;
use function implode;
use function parse_url;
use function str_ends_with;
use function str_starts_with;
use function strrpos;
use function substr;

final class RelativeUriResolver
{
    public function resolve(UriInterface $base, string $reference): UriInterface
    {
        $parts = parse_url($reference);

        if ($parts === false) {
            return $base;
        }

        if (array_key_exists('scheme', $parts)) {
            return $this->withComponents($base->withScheme($parts['scheme']), $parts);
        }

        if (array_key_exists('host', $parts)) {
            return $this->withComponents($base, $parts);
        }

        $query = array_key_exists('query', $parts) ? $parts['query'] : '';
        $fragment = $parts['fragment'] ?? '';
        $path = $parts['path'] ?? '';

        if ($path === '') {
            $resolved = array_key_exists('query', $parts) ? $base->withQuery($query) : $base;

            return $resolved->withFragment($fragment);
        }

        $mergedPath = str_starts_with($path, '/')
            ? $this->removeDotSegments($path)
            : $this->removeDotSegments($this->mergePaths($base, $path));

        return $base->withPath($mergedPath)->withQuery($query)->withFragment($fragment);
    }

    /**
     * @param array<string, int|string> $parts
     */
    private function withComponents(UriInterface $uri, array $parts): UriInterface
    {
        $uri = $uri
            ->withHost((string) ($parts['host'] ?? ''))
            ->withPath($this->removeDotSegments((string) ($parts['path'] ?? '')))
            ->withQuery((string) ($parts['query'] ?? ''))
            ->withFragment((string) ($parts['fragment'] ?? ''))
            ->withPort(array_key_exists('port', $parts) ? (int) $parts['port'] : null);

        if (array_key_exists('user', $parts)) {
            $uri = $uri->withUserInfo(
                (string) $parts['user'],
                array_key_exists('pass', $parts) ? (string) $parts['pass'] : null,
            );
        }

        return $uri;
    }

    private function mergePaths(UriInterface $base, string $relativePath): string
    {
        $basePath = $base->getPath();

        if ($base->getAuthority() !== '' && $basePath === '') {
            return '/' . $relativePath;
        }

        $lastSlash = strrpos($basePath, '/');

        if ($lastSlash === false) {
            return $relativePath;
        }

        return substr($basePath, 0, $lastSlash + 1) . $relativePath;
    }

    private function removeDotSegments(string $path): string
    {
        $input = explode('/', $path);

        /** @var list<string> $output */
        $output = [];

        foreach ($input as $segment) {
            if ($segment === '.') {
                continue;
            }

            if ($segment === '..') {
                array_pop($output);

                continue;
            }

            $output[] = $segment;
        }

        $resolved = implode('/', $output);

        if (str_starts_with($path, '/') && !str_starts_with($resolved, '/')) {
            $resolved = '/' . $resolved;
        }

        if ((str_ends_with($path, '/.') || str_ends_with($path, '/..')) && !str_ends_with($resolved, '/')) {
            $resolved .= '/';
        }

        return $resolved;
    }
}
