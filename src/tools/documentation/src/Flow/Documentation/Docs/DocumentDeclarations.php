<?php

declare(strict_types=1);

namespace Flow\Documentation\Docs;

use function array_key_exists;
use function mb_strtolower;
use function preg_match_all;

/**
 * Scope is the file, not the fence: declarations and calls sit in different blocks.
 */
final readonly class DocumentDeclarations
{
    /** @var array<string, true> */
    private array $names;

    /**
     * @param list<Fence> $fences
     */
    public function __construct(array $fences)
    {
        $names = [];

        foreach ($fences as $fence) {
            $matches = [];
            preg_match_all('/\bfunction\s+&?([a-zA-Z_\x80-\xff][a-zA-Z0-9_\x80-\xff]*)\s*\(/', $fence->code, $matches);

            foreach ($matches[1] as $name) {
                $names[mb_strtolower($name)] = true;
            }
        }

        $this->names = $names;
    }

    public function declares(string $name): bool
    {
        return array_key_exists(mb_strtolower($name), $this->names);
    }
}
