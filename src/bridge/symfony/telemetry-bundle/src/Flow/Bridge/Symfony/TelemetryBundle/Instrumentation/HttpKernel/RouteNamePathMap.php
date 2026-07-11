<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Symfony\Component\Config\ConfigCache;
use Symfony\Component\HttpKernel\CacheWarmer\CacheWarmerInterface;
use Symfony\Component\Routing\RouterInterface;
use Throwable;

use function dirname;
use function is_dir;
use function is_writable;
use function var_export;

/**
 * Deployment-static [route name => path template] map so request spans can carry the OTEL http.route
 * path template without touching the router at runtime — Router::getRouteCollection() bypasses the
 * compiled matcher and rebuilds the full route collection, which Symfony explicitly warns is too slow
 * for the request path.
 */
final class RouteNamePathMap implements CacheWarmerInterface
{
    private ConfigCache $cache;

    /** @var null|array<string, string> */
    private ?array $paths = null;

    private bool $unavailable = false;

    public function __construct(
        private readonly ?RouterInterface $router,
        string $directory,
        bool $debug,
    ) {
        $this->cache = new ConfigCache($directory . '/flow_telemetry_route_paths.php', $debug);
    }

    public function isOptional(): bool
    {
        return true;
    }

    public function pathFor(string $routeName): ?string
    {
        if ($this->paths === null && !$this->load()) {
            return null;
        }

        return $this->paths[$routeName] ?? null;
    }

    public function warmUp(string $cacheDir, ?string $buildDir = null): array
    {
        if ($this->router === null) {
            return [];
        }

        $collection = $this->router->getRouteCollection();
        $paths = [];

        foreach ($collection->all() as $name => $route) {
            $paths[$name] = $route->getPath();
        }

        $this->cache->write('<?php return ' . var_export($paths, true) . ';', $collection->getResources());
        $this->paths = $paths;

        return [$this->cache->getPath()];
    }

    private function load(): bool
    {
        if ($this->unavailable) {
            return false;
        }

        try {
            if (!$this->cache->isFresh()) {
                $directory = dirname($this->cache->getPath());

                // Guard against the unwritable branch before touching the router: under PHP-FPM the
                // failure memoization below lives one request only, so a throwing warmUp() would
                // rebuild the route collection on every request — the exact cost this map avoids.
                if (is_dir($directory) ? !is_writable($directory) : !is_writable(dirname($directory))) {
                    $this->unavailable = true;

                    return false;
                }

                $this->warmUp('');
            }
        } catch (Throwable) {
            $this->unavailable = true;

            return false;
        }

        if ($this->paths === null) {
            if (!$this->cache->isFresh()) {
                $this->unavailable = true;

                return false;
            }

            /** @var array<string, string> $paths */
            $paths = require $this->cache->getPath();
            $this->paths = $paths;
        }

        return true;
    }
}
