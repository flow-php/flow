<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Options;
use Flow\Filesystem\Path\UnixPath;
use Flow\Filesystem\Path\WindowsPath;
use Flow\Filesystem\Stream\ResourceContext;
use UnitEnum;

use function array_key_exists;
use function array_map;
use function constant;
use function Flow\Types\DSL\type_instance_of;
use function is_a;
use function is_array;
use function is_scalar;
use function is_string;

use const PHP_OS_FAMILY;

/**
 * @type PathData = array{uri: string, options: array<string, mixed>, partitions: list<array{name: string, value: null|string}>}
 */
final readonly class Path
{
    public function __construct(
        private WindowsPath|UnixPath $implementation,
        private Partitions $partitions = new Partitions(),
    ) {}

    /**
     * @param array<array-key, null|bool|float|int|string|\UnitEnum>|Options $options
     */
    public static function from(string $uri, array|Options $options = []): self
    {
        return new self(PHP_OS_FAMILY === 'Windows' ? new WindowsPath($uri, $options) : new UnixPath($uri, $options));
    }

    /**
     * Turn relative path into absolute paths even when path does not exists or it's glob pattern.
     *
     * @param array<string, null|bool|float|int|string|\UnitEnum>|Options $options
     *
     * @throws RuntimeException
     */
    public static function realpath(string $path, array|Options $options = []): self
    {
        return new self(
            PHP_OS_FAMILY === 'Windows' ? WindowsPath::realpath($path, $options) : UnixPath::realpath($path, $options),
        );
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(array $data): self
    {
        if (!array_key_exists('uri', $data) || !is_string($data['uri'])) {
            throw new InvalidArgumentException('Path data must hold a "uri" string');
        }

        if (!array_key_exists('options', $data) || !is_array($data['options'])) {
            throw new InvalidArgumentException('Path data must hold an "options" array');
        }

        if (!array_key_exists('partitions', $data) || !is_array($data['partitions'])) {
            throw new InvalidArgumentException('Path data must hold a "partitions" list');
        }

        /** @var array<string, null|bool|float|int|string|UnitEnum> $options */
        $options = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($data['options'] as $name => $value) {
            if (is_array($value) && array_key_exists('enum', $value) && array_key_exists('case', $value)) {
                if (
                    !is_string($value['enum'])
                    || !is_a($value['enum'], UnitEnum::class, true)
                    || !is_string($value['case'])
                ) {
                    throw new InvalidArgumentException("Path option \"{$name}\" names a class that is not an enum");
                }

                // @mago-ignore analysis:mixed-assignment
                $case = constant($value['enum'] . '::' . $value['case']);

                if (!$case instanceof UnitEnum) {
                    throw new InvalidArgumentException("Path option \"{$name}\" names a class that is not an enum");
                }

                $options[(string) $name] = $case;

                continue;
            }

            if ($value !== null && !is_scalar($value)) {
                throw new InvalidArgumentException("Path option \"{$name}\" must be a scalar, null or an enum");
            }

            $options[(string) $name] = $value;
        }

        $path = self::from($data['uri'], $options);

        if ($data['partitions'] === []) {
            return $path;
        }

        $partitions = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($data['partitions'] as $partition) {
            if (
                !is_array($partition)
                || !is_string($partition['name'] ?? null)
                || !(is_string($partition['value'] ?? null) || ($partition['value'] ?? null) === null)
            ) {
                throw new InvalidArgumentException('Path partitions must be a list of {name: string, value: ?string}');
            }

            $partitions[] = new Partition($partition['name'], $partition['value'] ?? null);
        }

        return $path->withPartitions(new Partitions(...$partitions));
    }

    public function addPartitions(Partition $partition, Partition ...$partitions): self
    {
        // B16: dropping $this->partitions here silently detached anything withPartitions() had attached
        return new self($this->implementation->addPartitions($partition, ...$partitions), $this->partitions);
    }

    public function basename(): string
    {
        return $this->implementation->basename();
    }

    public function basenamePrefix(string $prefix): self
    {
        return new self($this->implementation->basenamePrefix($prefix));
    }

    public function context(): ResourceContext
    {
        return ResourceContext::from($this);
    }

    public function endsWith(string $string): bool
    {
        return $this->implementation->endsWith($string);
    }

    public function extension(): string|false
    {
        return $this->implementation->extension();
    }

    /**
     * Extracts partitions from a concrete path by matching it against partition placeholders in this path.
     */
    public function extractPlaceholderPartitions(self $path): Partitions
    {
        if ($this->implementation instanceof UnixPath) {
            return $this->implementation->extractPlaceholderPartitions(type_instance_of(
                UnixPath::class,
            )->assert($path->implementation));
        }

        return $this->implementation->extractPlaceholderPartitions(type_instance_of(
            WindowsPath::class,
        )->assert($path->implementation));
    }

    public function filename(): string
    {
        return $this->implementation->filename();
    }

    public function getOption(
        string|Option $option,
        string|int|bool|float|UnitEnum|null $default = null,
    ): string|int|bool|float|UnitEnum|null {
        return $this->implementation->options()->get($option, $default);
    }

    /**
     * Path with partition placeholders replaced by glob wildcards, suitable for glob-based listing.
     */
    public function glob(): string
    {
        return $this->implementation->glob();
    }

    public function hasOption(string|Option $option): bool
    {
        return $this->implementation->options()->has($option);
    }

    public function isEqual(self $path): bool
    {
        if ($this->implementation instanceof UnixPath) {
            return $this->implementation->isEqual(type_instance_of(UnixPath::class)->assert($path->implementation));
        }

        return $this->implementation->isEqual(type_instance_of(WindowsPath::class)->assert($path->implementation));
    }

    public function isLocal(): bool
    {
        return $this->implementation->protocol() === 'file';
    }

    public function isPattern(): bool
    {
        return $this->implementation->isPattern();
    }

    public function matches(self $path): bool
    {
        if ($this->implementation instanceof UnixPath) {
            return $this->implementation->matches(type_instance_of(UnixPath::class)->assert($path->implementation));
        }

        return $this->implementation->matches(type_instance_of(WindowsPath::class)->assert($path->implementation));
    }

    /**
     * @return array<string, null|bool|float|int|string|\UnitEnum>
     */
    public function options(): array
    {
        return $this->implementation->options()->toArray();
    }

    public function parentDirectory(): self
    {
        return new self($this->implementation->parentDirectory());
    }

    /**
     * @return array<string>
     */
    public function partitionPlaceholders(): array
    {
        return $this->implementation->partitionPlaceholders();
    }

    /**
     * The uri, its options (enum values as {enum, case}) and the EXPLICIT partitions only - the ones attached with
     * withPartitions(), never the ones parsed from the uri.
     *
     * @return PathData
     */
    public function normalize(): array
    {
        $options = [];

        foreach ($this->implementation->options()->toArray() as $name => $value) {
            $options[$name] = $value instanceof UnitEnum ? ['enum' => $value::class, 'case' => $value->name] : $value;
        }

        $partitions = [];

        foreach ($this->partitions->toArray() as $partition) {
            $partitions[] = ['name' => $partition->name, 'value' => $partition->value];
        }

        return ['uri' => $this->uri(), 'options' => $options, 'partitions' => $partitions];
    }

    public function partitions(): Partitions
    {
        if (!$this->partitions->count()) {
            return $this->implementation->partitions();
        }

        return new Partitions(...$this->implementation->partitions()->toArray(), ...$this->partitions->toArray());
    }

    /**
     * @return array<Path>
     */
    public function partitionsPaths(): array
    {
        return array_map(static fn($implPath) => new self($implPath), $this->implementation->partitionsPaths());
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function path(): string
    {
        return $this->implementation->path();
    }

    public function protocol(): string
    {
        return $this->implementation->protocol();
    }

    public function randomize(): self
    {
        return new self($this->implementation->randomize());
    }

    public function rootDirectoryName(): ?string
    {
        return $this->implementation->rootDirectoryName();
    }

    public function setExtension(string $extension): self
    {
        return new self($this->implementation->setExtension($extension));
    }

    public function setOption(string|Option $option, string|int|bool|float|UnitEnum|null $value): self
    {
        return new self($this->implementation->withOptions($this->implementation->options()->set(
            $option instanceof Option ? $option->value : $option,
            $value,
        )));
    }

    public function setOptionWhenEmpty(string|Option $option, string|int|bool|float|UnitEnum|null $value): self
    {
        return new self($this->implementation->withOptions($this->implementation->options()->setWhenEmpty(
            $option,
            $value,
        )));
    }

    public function skipDirectories(int $count): ?self
    {
        return ($newImplementation = $this->implementation->skipDirectories($count)) === null
            ? null
            : new self($newImplementation);
    }

    public function staticPart(): self
    {
        return new self($this->implementation->staticPart());
    }

    public function suffix(string $string): self
    {
        return new self($this->implementation->suffix($string));
    }

    /**
     * Difference between Path::uri and Path::path is that Path::uri returns path with scheme and Path::path returns path without scheme.
     */
    public function uri(): string
    {
        return $this->implementation->uri();
    }

    /**
     * Attach explicit partitions to the path, merged with partitions parsed from the path itself.
     * Used to carry partitions extracted from partition placeholders on concrete, listed paths.
     */
    public function withPartitions(Partitions $partitions): self
    {
        return new self($this->implementation, $partitions);
    }
}
