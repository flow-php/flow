<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\Filesystem\Path;

use function bin2hex;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_non_empty_string;
use function in_array;
use function max;
use function preg_replace;
use function random_bytes;
use function sprintf;
use function str_contains;
use function str_pad;
use function str_repeat;
use function str_replace;
use function strlen;
use function strrpos;
use function substr;

use const STR_PAD_LEFT;

/**
 * One task of a file write: the {part} it fills, zero-padded, and the run it belongs to, appended to the name under
 * Append. One naming rule for the serial writer (part 0) and the parallel commit.
 */
final readonly class Part
{
    public const string PLACEHOLDER = '{part}';

    /**
     * The length of a serial run id (8 random bytes, hex): what an appended sibling of a plain file carries.
     */
    public const int RUN_LENGTH = 16;

    /**
     * @param int<0, max> $index
     * @param int<1, max> $splits
     * @param non-empty-string $run
     */
    public function __construct(
        public int $index,
        public int $splits,
        public string $run,
        public bool $append,
    ) {}

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        return new self(
            max(0, type_integer()->assert($data['index'] ?? null)),
            max(1, type_integer()->assert($data['splits'] ?? null)),
            type_non_empty_string()->assert($data['run'] ?? null),
            type_boolean()->assert($data['append'] ?? null),
        );
    }

    /**
     * {part} and every partition placeholder as *, plus **\/ before the basename when $partitioned: what an existing
     * part of $destination matches.
     */
    public static function glob(Path $destination, bool $partitioned): Path
    {
        $uri = $destination->uri();
        $slash = (int) strrpos($uri, '/');
        $directory = preg_replace('/\{[^\/{}]+\}/', '*', substr($uri, 0, $slash)) ?? '';
        $basename = preg_replace('/\{[^\/{}]+\}/', '*', substr($uri, $slash + 1)) ?? '';

        return Path::from($directory . ($partitioned ? '/**/' : '/') . $basename, $destination->options());
    }

    /**
     * @return non-empty-string a fresh run id for a serial write
     */
    public static function serialRun(): string
    {
        return bin2hex(random_bytes(self::RUN_LENGTH / 2));
    }

    /**
     * The globs of the existing files that $file, written for $destination, replaces under Overwrite: every part in
     * $file's directory for a {part} destination (the whole destination when it is not partitioned), $file itself
     * and its appended siblings (<name>-<run>.<ext>) otherwise - never a file Flow did not name.
     *
     * @return list<Path>
     */
    public static function stale(Path $file, Path $destination): array
    {
        if (!self::in($destination)) {
            return [
                $file,
                Path::from(
                    $file->parentDirectory()->uri()
                    . '/'
                    . $file->filename()
                    . '-'
                    . str_repeat('?', self::RUN_LENGTH)
                    . '.'
                    . (string) $file->extension(),
                    $file->options(),
                ),
            ];
        }

        if ($file->partitions()->count() > 0 || [] !== $destination->partitionPlaceholders()) {
            return [self::glob(
                Path::from($file->parentDirectory()->uri() . '/' . $destination->basename(), $destination->options()),
                false,
            )];
        }

        return [self::glob($destination, false)];
    }

    public static function in(Path $destination): bool
    {
        return str_contains($destination->path(), self::PLACEHOLDER);
    }

    /**
     * @throws InvalidArgumentException when {part} is in $destination and 'part' is a partition column
     */
    public static function refuseReserved(Path $destination, Partitioning $partitioning): void
    {
        if (self::in($destination) && in_array('part', $partitioning->by->names(), true)) {
            throw InvalidArgumentException::because(
                '"{part}" in destination %s is the split part and cannot name the partition column "part"; rename the column',
                $destination->uri(),
            );
        }
    }

    /**
     * $destination with {part} replaced by name(); unchanged without the placeholder.
     *
     * @throws InvalidArgumentException when {part} is outside the basename
     */
    public function fill(Path $destination): Path
    {
        if (!self::in($destination)) {
            return $destination;
        }

        $uri = $destination->uri();
        $slash = (int) strrpos($uri, '/');

        if (str_contains(substr($uri, 0, $slash), self::PLACEHOLDER)) {
            throw InvalidArgumentException::because('"{part}" names a file, not a directory, given: %s', $uri);
        }

        return Path::from(
            substr($uri, 0, $slash) . '/' . str_replace(self::PLACEHOLDER, $this->name(), substr($uri, $slash + 1)),
            $destination->options(),
        );
    }

    /**
     * $file under Append when it exists already: <name>-<run>.<ext> beside it.
     */
    public function beside(Path $file): Path
    {
        return Path::from(
            $file->parentDirectory()->uri()
            . '/'
            . $file->filename()
            . '-'
            . $this->run
            . '.'
            . (string) $file->extension(),
            $file->options(),
        );
    }

    /**
     * The index zero-padded to max(5, digits of the last split), then "-<run>" under Append: 00003,
     * 00003-1a2b3c4d5e6f7a8b.
     */
    public function name(): string
    {
        $name = str_pad((string) $this->index, max(5, strlen((string) ($this->splits - 1))), '0', STR_PAD_LEFT);

        return $this->append ? sprintf('%s-%s', $name, $this->run) : $name;
    }

    /**
     * @return array{index: int<0, max>, splits: int<1, max>, run: non-empty-string, append: bool}
     */
    public function normalize(): array
    {
        return ['index' => $this->index, 'splits' => $this->splits, 'run' => $this->run, 'append' => $this->append];
    }
}
