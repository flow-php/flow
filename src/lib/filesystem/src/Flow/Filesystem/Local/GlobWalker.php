<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\Path;
use Flow\Filesystem\Path\GlobPattern;

use function array_filter;
use function array_keys;
use function array_map;
use function array_pop;
use function array_shift;
use function array_values;
use function count;
use function explode;
use function implode;
use function is_dir;
use function is_link;
use function is_readable;
use function preg_replace;
use function rtrim;
use function scandir;
use function sort;
use function strpbrk;

final readonly class GlobWalker
{
    /**
     * @return list<string> every file and directory the pattern matches, sorted
     */
    public function walk(Path $pattern): array
    {
        $segments = explode('/', preg_replace('#/+#', '/', $pattern->path()) ?? $pattern->path());
        $static = [];

        while ($segments !== [] && strpbrk($segments[0], '*?[{') === false) {
            $static[] = array_shift($segments);
        }

        $base = implode('/', $static) ?: '/';

        if ($segments === [] || !is_dir($base) || !is_readable($base)) {
            return [];
        }

        // ** repeated matches exactly what one ** matches, and every extra one would walk the tree again
        $collapsed = [];

        foreach ($segments as $segment) {
            if ($segment !== '**' || ($collapsed[count($collapsed) - 1] ?? null) !== '**') {
                $collapsed[] = $segment;
            }
        }

        $matchers = array_map(static fn(string $segment): GlobPattern => new GlobPattern($segment), $collapsed);
        $last = count($collapsed) - 1;
        $found = [];
        $queue = [[$base, 0]];

        while ($queue !== []) {
            [$directory, $index] = array_pop($queue);
            $spanning = $collapsed[$index] === '**';
            // a whole-segment ** also matches zero directories, so the segment after it is tried in the same pass
            $explicit = $spanning ? ($index < $last ? $index + 1 : null) : $index;

            foreach (scandir($directory) ?: [] as $name) {
                if ($name === '.' || $name === '..') {
                    continue;
                }

                $path = rtrim($directory, '/') . '/' . $name;

                if ($spanning) {
                    if ($index === $last) {
                        $found[$path] = true;
                    }

                    // ** never follows a symlinked directory - a link back up would never end
                    if (is_dir($path) && !is_link($path) && is_readable($path)) {
                        $queue[] = [$path, $index];
                    }
                }

                if ($explicit === null || !$matchers[$explicit]->matches($name)) {
                    continue;
                }

                if ($explicit === $last) {
                    $found[$path] = true;
                } elseif (is_dir($path) && is_readable($path)) {
                    $queue[] = [$path, $explicit + 1];
                }
            }
        }

        $paths = array_values(array_filter(
            array_keys($found),
            (new GlobPattern(rtrim($base, '/') . '/' . implode('/', $collapsed)))->matches(...),
        ));
        sort($paths, SORT_STRING);

        return $paths;
    }
}
