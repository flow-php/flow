<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Context;

use Flow\Benchmarks\Datasets\DigestCache;
use Flow\Benchmarks\Datasets\Paths;

use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function is_dir;
use function mkdir;
use function putenv;
use function symlink;
use function uniqid;

/**
 * Repoints FLOW_MONOREPO_PROJECT_ROOT at a throwaway root so Paths can be exercised without
 * reaching the real benchmarks/datasets or benchmarks/var.
 */
final readonly class ProjectRoot
{
    private string $original;

    private string $root;

    public function __construct(string $name)
    {
        $this->original = Paths::projectRoot();
        // Deliberately outside benchmarks/var: a root nested inside it would make every decoy
        // realpath contain '/benchmarks/var' and Datasets::reset()'s guard could never be exercised.
        $this->root = $this->original . '/var/benchmark-project-root/' . $name . '_' . uniqid('', true);

        mkdir($this->root . '/benchmarks', 0777, true);

        putenv('FLOW_MONOREPO_PROJECT_ROOT=' . $this->root);
        DigestCache::clear();
    }

    /**
     * Makes benchmarks/var a symlink to a directory whose real path is outside benchmarks/var, so
     * Datasets::reset() finds a directory that exists but must refuse to delete it.
     */
    public function linkVarOutside(): string
    {
        $decoy = $this->root . '/decoy';
        mkdir($decoy, 0777, true);
        symlink($decoy, $this->root . '/benchmarks/var');

        return $decoy;
    }

    public function release(): void
    {
        putenv('FLOW_MONOREPO_PROJECT_ROOT=' . $this->original);
        DigestCache::clear();

        if (is_dir($this->root)) {
            native_local_filesystem()->rm(path($this->root));
        }
    }
}
