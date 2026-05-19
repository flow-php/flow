<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;
use Psr\Container\ContainerInterface;

use function array_keys;
use function array_pop;
use function explode;
use function getcwd;
use function implode;
use function method_exists;
use function preg_match;
use function realpath;
use function sprintf;
use function str_starts_with;

final readonly class FstabResolver
{
    public function __construct(
        private ContainerInterface $locator,
        private string $defaultFstab,
    ) {}

    /**
     * @return list<string>
     */
    public function availableFstabs(): array
    {
        if (!method_exists($this->locator, 'getProvidedServices')) {
            return [];
        }

        /** @var array<string, string> $services */
        $services = $this->locator->getProvidedServices();

        return array_keys($services);
    }

    public function defaultFstabName(): string
    {
        return $this->defaultFstab;
    }

    public function parseUri(string $raw): Path
    {
        if (preg_match('#^[a-zA-Z][a-zA-Z0-9+.-]*://#', $raw) === 1) {
            return Path::from($raw);
        }

        $absolute = str_starts_with($raw, '/') ? $raw : getcwd() . '/' . $raw;
        $real = realpath($absolute);

        return Path::from('file://' . ($real !== false ? $real : self::normalizePath($absolute)));
    }

    public function resolve(?string $fstabName): FilesystemTable
    {
        $name = $fstabName ?? $this->defaultFstab;

        if ($name === '' || !$this->locator->has($name)) {
            throw new InvalidArgumentException(sprintf(
                'Unknown fstab "%s". Available fstabs: [%s].',
                $name,
                implode(', ', $this->availableFstabs()),
            ));
        }

        /** @phpstan-ignore return.type */
        return $this->locator->get($name);
    }

    private static function normalizePath(string $path): string
    {
        $isAbsolute = str_starts_with($path, '/');
        $segments = [];

        foreach (explode('/', $path) as $segment) {
            if ($segment === '' || $segment === '.') {
                continue;
            }

            if ($segment === '..') {
                array_pop($segments);

                continue;
            }

            $segments[] = $segment;
        }

        return ($isAbsolute ? '/' : '') . implode('/', $segments);
    }
}
