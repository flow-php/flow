<?php

declare(strict_types=1);

namespace Flow\ETL\Storage;

use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Storage\Floe\FloeStreamWriter;
use Flow\ETL\Storage\Floe\Footer;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Generator;
use SensitiveParameter;

use function array_key_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function hash;
use function max;

final class FilesystemRowsStorage implements RowsStorage, Describable
{
    private readonly Path $cacheDir;

    private readonly FloeFiles $files;

    private readonly Path $root;

    /**
     * @var array<string, FloeStreamWriter>
     */
    private array $writers = [];

    /**
     * @param null|Path $cacheDir the filesystem's temporary directory when null
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        ?Path $cacheDir,
        private readonly Backend $backend,
        private readonly int $batchSize = 2048,
    ) {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be at least 1');
        }

        $this->root = $cacheDir ?? $this->filesystem->getSystemTmpDir();
        $this->cacheDir = $this->root->suffix('/flow-php-rows/');
        $this->files = new FloeFiles($this->filesystem, $backend);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
            Path::fromArray(type_array()->assert($data['root'] ?? null)),
            $rebuild->of($data['backend'] ?? null, Backend::class),
            max(1, type_integer()->assert($data['batch_size'] ?? null)),
        );
    }

    /**
     * A key holds one schema: the first batch's. Later batches must fit it.
     */
    public function append(string $key, Rows $rows): void
    {
        ($this->writers[$key] ??= $this->files->append($this->keyPath($key), $rows->schema()))->write($rows);
    }

    public function close(string $key): void
    {
        $this->closeWriter($key);
    }

    public function describe(): ?Description
    {
        if (!$this->backend instanceof Describable) {
            return null;
        }

        return Description::compose(self::class, [
            'filesystem' => new ConfiguredFilesystem($this->filesystem),
            'root' => $this->root->normalize(),
            'backend' => $this->backend,
            'batch_size' => $this->batchSize,
        ]);
    }

    public function get(string $key): Generator
    {
        $this->closeWriter($key);

        $path = $this->keyPath($key);

        if (!$this->filesystem->status($path)) {
            return;
        }

        $reader = $this->files->read($path);

        // finally, not a trailing close(): PHP runs it on generator destruction too, and a KWayMerge cursor is
        // destroyed rather than exhausted when a merge throws
        // every append() is one Floe frame and a read never spans two, so small appends are coalesced back into
        // batches of up to batchSize rows
        $pending = [];
        $pendingRows = 0;

        try {
            foreach ($reader->rows($this->batchSize) as $batch) {
                if (($pendingRows + $batch->count()) > $this->batchSize) {
                    yield Rows::concat($pending[0]->schema(), $this->backend, ...$pending);
                    $pending = [];
                    $pendingRows = 0;
                }

                $pending[] = $batch;
                $pendingRows += $batch->count();
            }

            if ($pending !== []) {
                yield Rows::concat($pending[0]->schema(), $this->backend, ...$pending);
            }
        } finally {
            $reader->close();
        }
    }

    /**
     * A key whose file has no footer - a write cut short by a killed process - is missing.
     */
    public function has(string $key): bool
    {
        return array_key_exists($key, $this->writers) || $this->footer($key) !== null;
    }

    public function remove(string $key): void
    {
        $this->closeWriter($key);

        $this->filesystem->rm($this->keyPath($key)->parentDirectory());
    }

    public function rows(string $key): Cardinality
    {
        $this->closeWriter($key);

        return Cardinality::exact($this->footer($key)?->statistics->rows ?? 0);
    }

    public function schema(string $key): ?Schema
    {
        $this->closeWriter($key);

        return $this->footer($key)?->schema();
    }

    /**
     * The row offset each Floe section of the key starts at, in order; [] for a missing key (closes its writer
     * first).
     *
     * @return list<int>
     */
    public function sections(string $key): array
    {
        $this->closeWriter($key);

        $offsets = [];
        $rows = 0;

        foreach ($this->footer($key)->sections ?? [] as $section) {
            $offsets[] = $rows;
            $rows += $section->rowCount;
        }

        return $offsets;
    }

    /**
     * The key's rows from row $offset on, as read (no coalescing); seeks by section.
     *
     * @param int<0, max> $offset
     * @param null|int<1, max> $limit
     *
     * @return Generator<Rows>
     */
    public function slice(string $key, int $offset, ?int $limit = null): Generator
    {
        $this->closeWriter($key);

        $path = $this->keyPath($key);

        if (!$this->filesystem->status($path)) {
            return;
        }

        $reader = $this->files->read($path);

        try {
            yield from $reader->rows($this->batchSize, $offset, $limit);
        } finally {
            $reader->close();
        }
    }

    private function closeWriter(string $key): void
    {
        if (!array_key_exists($key, $this->writers)) {
            return;
        }

        $writer = $this->writers[$key];
        unset($this->writers[$key]);

        $writer->close();
    }

    private function footer(string $key): ?Footer
    {
        $path = $this->keyPath($key);

        if (!$this->filesystem->status($path)) {
            return null;
        }

        $reader = $this->files->read($path);

        try {
            return $reader->footer();
        } catch (FloeException) {
            return null;
        } finally {
            $reader->close();
        }
    }

    private function keyPath(string $key): Path
    {
        return $this->cacheDir->suffix(hash('xxh128', $key) . '/' . $key . '.floe');
    }
}
