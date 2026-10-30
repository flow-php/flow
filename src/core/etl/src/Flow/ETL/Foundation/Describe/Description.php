<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Describe;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use SensitiveParameter;

use function array_key_exists;
use function array_merge;
use function array_unique;
use function array_values;
use function array_walk_recursive;
use function is_array;
use function is_bool;
use function is_object;
use function is_string;
use function sort;

/**
 * @type DeclaredSetting = array{value: string, class: string}
 */
final readonly class Description
{
    /**
     * @param class-string $class a Describable
     * @param array<array-key, mixed> $data
     * @param list<string> $extensions the PHP extensions $class needs where it is rebuilt, lower-case module names
     * @param array<string, string> $settings the php.ini settings $class needs where it is rebuilt
     */
    public function __construct(
        public string $class,
        #[SensitiveParameter]
        public array $data,
        public bool $secret = false,
        public array $extensions = [],
        public array $settings = [],
    ) {}

    /**
     * @param array<array-key, mixed> $data
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(#[SensitiveParameter] array $data): self
    {
        foreach (['class', 'secret', 'data'] as $key) {
            if (!array_key_exists($key, $data)) {
                throw InvalidArgumentException::because('Description is missing the "%s" key', $key);
            }
        }

        if (!is_string($data['class']) || $data['class'] === '') {
            throw new InvalidArgumentException('Description "class" must be a non-empty string');
        }

        if (!is_bool($data['secret'])) {
            throw new InvalidArgumentException('Description "secret" must be a boolean');
        }

        if (!is_array($data['data'])) {
            throw new InvalidArgumentException('Description "data" must be an array');
        }

        /** @var class-string $class */
        $class = $data['class'];

        return new self($class, $data['data'], $data['secret']);
    }

    /**
     * new self($class, $data, $secret) with every Describable inside $data (at any depth of arrays) replaced by its
     * description; null when one of them describes to null. Any other object in $data is refused: callers pass enums
     * as their case name and Type/Schema/Path/Partitioning already normalized.
     *
     * @param class-string $class
     * @param array<array-key, mixed> $data
     * @param list<string> $extensions
     * @param array<string, string> $settings
     *
     * @throws InvalidArgumentException
     */
    public static function compose(
        string $class,
        #[SensitiveParameter]
        array $data,
        bool $secret = false,
        array $extensions = [],
        array $settings = [],
    ): ?self {
        $composed = self::composeData($data, $class);

        return $composed === null ? null : new self($class, $composed, $secret, $extensions, $settings);
    }

    /**
     * @param array<string, DeclaredSetting> $settings
     * @param array<string, DeclaredSetting> $other
     *
     * @throws InvalidArgumentException two classes declare different values for one setting
     *
     * @return array<string, DeclaredSetting>
     */
    public static function unionSettings(array $settings, array $other): array
    {
        foreach ($other as $key => $declared) {
            if (!array_key_exists($key, $settings)) {
                $settings[$key] = $declared;

                continue;
            }

            if ($settings[$key]['value'] !== $declared['value']) {
                throw InvalidArgumentException::because(
                    '%s declares the worker setting "%s" as "%s", %s declares it as "%s"',
                    $settings[$key]['class'],
                    $key,
                    $settings[$key]['value'],
                    $declared['class'],
                    $declared['value'],
                );
            }
        }

        return $settings;
    }

    /**
     * $data with every Describable inside it (at any depth of arrays) replaced by its description; null when one of
     * them describes to null. Any other object is refused, named with $class and its key.
     *
     * @param array<array-key, mixed> $data
     * @param class-string $class the class describing $data
     *
     * @throws InvalidArgumentException
     *
     * @return null|array<array-key, mixed>
     */
    public static function composeData(#[SensitiveParameter] array $data, string $class): ?array
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($data as $key => $value) {
            if ($value instanceof Describable) {
                $description = $value->describe();

                if ($description === null) {
                    return null;
                }

                $data[$key] = $description;
            } elseif (is_array($value)) {
                $nested = self::composeData($value, $class);

                if ($nested === null) {
                    return null;
                }

                $data[$key] = $nested;
            } elseif (is_object($value) && !$value instanceof self) {
                throw InvalidArgumentException::because(
                    '%s describes "%s" as a %s, which is neither Describable nor a Description',
                    $class,
                    (string) $key,
                    $value::class,
                );
            }
        }

        return $data;
    }

    /**
     * @return array{class: string, secret: bool, data: array<array-key, mixed>|string}
     */
    public function __debugInfo(): array
    {
        return [
            'class' => $this->class,
            'secret' => $this->sensitive(),
            'data' => $this->sensitive() ? '[redacted]' : $this->data,
        ];
    }

    /**
     * @return list<string> its own and every nested description's, sorted, unique
     */
    public function extensions(): array
    {
        $extensions = $this->extensions;
        $data = $this->data;

        array_walk_recursive($data, static function (mixed $value) use (&$extensions): void {
            if ($value instanceof self) {
                $extensions = array_merge($extensions, $value->extensions());
            }
        });

        $extensions = array_values(array_unique($extensions));
        sort($extensions);

        return $extensions;
    }

    /**
     * @throws InvalidArgumentException two descriptions declare different values for one setting
     *
     * @return array<string, DeclaredSetting> its own and every nested description's, by setting
     */
    public function settings(): array
    {
        $settings = [];

        foreach ($this->settings as $key => $value) {
            $settings[$key] = ['value' => $value, 'class' => $this->class];
        }

        $nested = [];
        $data = $this->data;

        array_walk_recursive($data, static function (mixed $value) use (&$nested): void {
            if ($value instanceof self) {
                $nested[] = $value;
            }
        });

        foreach ($nested as $description) {
            $settings = self::unionSettings($settings, $description->settings());
        }

        return $settings;
    }

    /**
     * @return array{class: string, secret: bool, data: array<array-key, mixed>}
     */
    public function normalize(): array
    {
        $normalize = static function (array $data) use (&$normalize): array {
            // @mago-ignore analysis:mixed-assignment
            foreach ($data as $key => $value) {
                if ($value instanceof self) {
                    $data[$key] = $value->normalize();
                } elseif (is_array($value)) {
                    $data[$key] = $normalize($value);
                }
            }

            return $data;
        };

        return [
            'class' => $this->class,
            'secret' => $this->sensitive(),
            'data' => $normalize($this->data),
        ];
    }

    public function sensitive(): bool
    {
        if ($this->secret) {
            return true;
        }

        $sensitive = false;
        $data = $this->data;

        array_walk_recursive($data, static function (mixed $value) use (&$sensitive): void {
            if ($value instanceof self && $value->sensitive()) {
                $sensitive = true;
            }
        });

        return $sensitive;
    }
}
