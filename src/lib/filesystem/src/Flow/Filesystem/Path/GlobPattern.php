<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use function count;
use function preg_match;
use function preg_quote;
use function preg_replace;
use function preg_split;
use function str_contains;
use function str_split;
use function strcmp;
use function strlen;
use function strpos;
use function substr;

final readonly class GlobPattern
{
    private string $byteRegex;

    private string $regex;

    public function __construct(string $pattern)
    {
        $pattern = self::oneSeparator($pattern);
        // a pattern that is not valid UTF-8 can only be read byte by byte
        $unicode = preg_match('//u', $pattern) === 1;
        $regex = '';
        $placeholder = [];
        $length = strlen($pattern);

        for ($i = 0; $i < $length; $i++) {
            $char = $pattern[$i];
            $atSegmentStart = $i === 0 || $pattern[$i - 1] === '/';
            $endsPattern = ($i + 2) === $length;

            if (
                $char === '*'
                && $atSegmentStart
                && substr($pattern, $i, 2) === '**'
                && ($endsPattern || $pattern[$i + 2] === '/')
            ) {
                $regex .= $endsPattern ? '.*' : '(?:[^/]+/)*';
                $i += $endsPattern ? 1 : 2;

                continue;
            }

            if ($char === '*') {
                $regex .= '[^/]*';

                continue;
            }

            if ($char === '?') {
                $regex .= '[^/]';

                continue;
            }

            if ($char === '{' && preg_match('/\G\{[^\/\\\\{}*?\[\]]+\}/', $pattern, $placeholder, 0, $i) === 1) {
                $regex .= '[^/]+';
                $i += strlen($placeholder[0]) - 1;

                continue;
            }

            if ($char === '[') {
                $negated = ($pattern[$i + 1] ?? '') === '!';
                $from = $negated ? $i + 2 : $i + 1;
                // a ] right after [ or [! belongs to the set
                $close = strpos($pattern, ']', ($pattern[$from] ?? '') === ']' ? $from + 1 : $from);
                $set = $close === false ? '/' : substr($pattern, $from, $close - $from);

                // a set never spans a directory boundary, so a [ without a ] in its own segment is a literal
                if ($close !== false && !str_contains($set, '/')) {
                    $members = $unicode ? preg_split('//u', $set, -1, PREG_SPLIT_NO_EMPTY) : str_split($set);
                    $members = $members === false ? [] : $members;
                    $class = '';
                    $count = count($members);

                    for ($m = 0; $m < $count; $m++) {
                        if (($members[$m + 1] ?? null) === '-' && ($m + 2) < $count) {
                            // a reversed range matches nothing; UTF-8 byte order is code point order
                            if (strcmp($members[$m], $members[$m + 2]) <= 0) {
                                $class .= preg_quote($members[$m], '~') . '-' . preg_quote($members[$m + 2], '~');
                            }

                            $m += 2;

                            continue;
                        }

                        $class .= preg_quote($members[$m], '~');
                    }

                    // a class never matches /, even through a range such as [.-0]
                    $regex .= match (true) {
                        $negated => '[^/' . $class . ']',
                        $class === '' => '(?!)',
                        default => '(?!/)[' . $class . ']',
                    };
                    $i = $close;

                    continue;
                }
            }

            $regex .= preg_quote($char, '~');
        }

        $this->byteRegex = '~^' . $regex . '$~';
        $this->regex = $unicode ? $this->byteRegex . 'u' : $this->byteRegex;
    }

    public function matches(string $path): bool
    {
        $path = self::oneSeparator($path);
        $matched = preg_match($this->regex, $path);

        // a path that is not valid UTF-8 can only be matched byte by byte
        return ($matched === false ? preg_match($this->byteRegex, $path) : $matched) === 1;
    }

    private static function oneSeparator(string $path): string
    {
        return preg_replace('#/+#', '/', $path) ?? $path;
    }
}
