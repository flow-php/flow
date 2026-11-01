<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use function array_key_exists;
use function get_defined_functions;
use function mb_strrpos;
use function mb_strtolower;
use function mb_substr;

/**
 * Composer `files` loads every functions.php, so defined functions = published DSL; short names for imported calls.
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
