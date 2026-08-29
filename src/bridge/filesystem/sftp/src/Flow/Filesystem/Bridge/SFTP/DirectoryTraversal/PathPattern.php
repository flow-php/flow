<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\DirectoryTraversal;

use Flow\Filesystem\Path;

use function array_slice;
use function count;
use function explode;
use function Flow\Filesystem\DSL\path;
use function implode;
use function str_contains;
use function trim;

final readonly class PathPattern
{
    private bool $crossesDirectories;

    private int $depth;

    /** @var array<int, string> */
    private array $segments;

    public function __construct(
        private Path $path,
    ) {
        $this->segments = self::segments($path->path());
        $this->depth = count($this->segments);
        $this->crossesDirectories = str_contains($path->path(), '**');
    }

    public function accepts(Path $candidate): bool
    {
        return !$this->path->isPattern() || $this->path->matches($candidate);
    }

    public function mayContainMatches(string $remoteDirectory): bool
    {
        if (!$this->path->isPattern() || $this->crossesDirectories) {
            return true;
        }

        $candidateDepth = count(self::segments($remoteDirectory));

        if ($candidateDepth >= $this->depth) {
            return false;
        }

        return $this->truncatedTo($candidateDepth)->matches($this->pathTo($remoteDirectory));
    }

    public function pathTo(string $remotePath): Path
    {
        return path($this->path->protocol() . '://' . $remotePath, $this->path->options());
    }

    /**
     * @return array<int, string>
     */
    private static function segments(string $path): array
    {
        $trimmed = trim($path, '/');

        return $trimmed === '' ? [] : explode('/', $trimmed);
    }

    private function truncatedTo(int $depth): Path
    {
        return path(
            $this->path->protocol() . ':///' . implode('/', array_slice($this->segments, 0, $depth)),
            $this->path->options(),
        );
    }
}
