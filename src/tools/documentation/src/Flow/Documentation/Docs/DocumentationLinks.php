<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use RuntimeException;

use function explode;
use function file_exists;
use function file_get_contents;
use function is_dir;
use function preg_match_all;
use function rtrim;
use function sprintf;
use function str_starts_with;
use function strlen;
use function substr;

/**
 * Both spellings occur in the corpus - the site-relative `/documentation/a/b` and the absolute
 * `https://flow-php.com/documentation/a/b` - and a checker that understood only one would miss every
 * dead link written the other way.
 */
final readonly class DocumentationLinks
{
    private const SITE = 'https://flow-php.com';

    /**
     * Paths the site serves from generated data rather than from a file under documentation/:
     * the phpDocumentor dump, the DSL reference built from dsl.json, and the example corpus.
     */
    private const GENERATED = [
        '/documentation/api/',
        '/documentation/dsl/',
        '/documentation/example/',
        '/documentation/examples',
    ];

    /**
     * @return list<array{file: string, line: int, href: string}>
     */
    public function of(string $file): array
    {
        $contents = file_get_contents($file);

        if ($contents === false) {
            throw new RuntimeException(sprintf('Unable to read "%s".', $file));
        }

        $links = [];

        foreach (explode("\n", $contents) as $index => $line) {
            $matches = [];
            preg_match_all('/\]\(([^)\s]+)\)/', $line, $matches);

            foreach ($matches[1] as $href) {
                if ($this->isDocumentationHref($href)) {
                    $links[] = ['file' => $file, 'line' => $index + 1, 'href' => $href];
                }
            }
        }

        return $links;
    }

    public function isDocumentationHref(string $href): bool
    {
        return str_starts_with($href, '/documentation/') || str_starts_with($href, self::SITE . '/documentation/');
    }

    public function resolves(string $href): bool
    {
        $path = str_starts_with($href, self::SITE) ? substr($href, strlen(self::SITE)) : $href;
        $path = explode('#', $path)[0];
        $path = rtrim($path, '/');

        if ($path === '/documentation') {
            return true;
        }

        foreach (self::GENERATED as $prefix) {
            if (str_starts_with($path . '/', $prefix)) {
                return true;
            }
        }

        $relative = substr($path, 1);

        return file_exists($relative) || file_exists($relative . '.md') || is_dir($relative);
    }
}
