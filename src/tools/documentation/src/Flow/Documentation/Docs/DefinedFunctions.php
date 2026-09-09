<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use function array_key_exists;
use function get_defined_functions;
use function mb_strrpos;
use function mb_strtolower;
use function mb_substr;

/**
 * Composer loads every `functions.php` in the monorepo through its `files` entry, so by the time a
 * test runs the set of defined user functions IS the set of published DSL functions - it cannot be
 * served stale.
 *
 * Short names are indexed alongside qualified ones because a fence that imports a function calls it
 * unqualified.
 */
final readonly class DefinedFunctions
{
    /** @var array<string, true> */
    private array $names;

    public function __construct()
    {
        $names = [];

        foreach (get_defined_functions()['user'] as $function) {
            $names[mb_strtolower($function)] = true;
            $separator = mb_strrpos($function, '\\');

            if ($separator !== false) {
                $names[mb_strtolower(mb_substr($function, $separator + 1))] = true;
            }
        }

        $this->names = $names;
    }

    public function has(string $name): bool
    {
        return array_key_exists(mb_strtolower($name), $this->names);
    }
}
