<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Flow\ETL\Config\ConfigBuilder;
use Flow\Filesystem\Filesystem;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\Filesystem\DSL\filesystem_telemetry_config;
use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\traceable_filesystem;
use function Flow\Types\DSL\type_string;
use function ltrim;

final readonly class FlowConfigFactory
{
    public function __construct(
        private Telemetry $telemetry,
        private ContainerBagInterface $parameters,
    ) {}

    /**
     * Create a ConfigBuilder with telemetry pre-configured.
     */
    public function configBuilder(string $name): ConfigBuilder
    {
        return config_builder()
            ->name($name)
            ->withTelemetry(
                $this->telemetry,
                telemetry_options()->collectMetrics()->traceLoading()->traceCache()->traceTransformations(),
            );
    }

    /**
     * Create a ConfigBuilder with telemetry and cache pre-configured.
     *
     * @param int $ttl Cache TTL in seconds (default: 24 hours)
     */
    public function configBuilderWithCache(string $name, int $ttl = 86400): ConfigBuilder
    {
        return $this->configBuilder($name)->cache($this->cache($name, $ttl));
    }

    public function filesystem(): Filesystem
    {
        return traceable_filesystem(native_local_filesystem(), filesystem_telemetry_config(
            $this->telemetry,
            new SystemClock(),
            filesystem_telemetry_options()->collectMetrics()->traceStreams(),
        ));
    }

    public function telemetry(): Telemetry
    {
        return $this->telemetry;
    }

    private function cache(string $directoryName, int $ttl): PSRSimpleCache
    {
        return new PSRSimpleCache(new Psr16Cache(
            new FilesystemAdapter(
                'flow-contributors',
                $ttl,
                directory: type_string()->assert($this->parameters->get('kernel.cache_dir')) . '/'
                    . ltrim($directoryName, '/'),
            ),
        ));
    }
}
