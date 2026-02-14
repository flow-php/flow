<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use function Flow\ETL\DSL\{config_builder, df, from_cache, lit, not, ref, rename_replace, telemetry_options, to_memory};
use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TraceableCacheAdapter;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Flow\ETL\Memory\ArrayMemory;
use Flow\Telemetry\Telemetry;
use Flow\Website\Factory\Github\ContributorsRequestFactory;
use Http\Client\Curl\Client;
use Http\Discovery\Psr17Factory;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

final readonly class Github
{
    public function __construct(
        private ContributorsRequestFactory $requestFactory,
        private ContainerBagInterface $parameters,
        private Telemetry $telemetry,
    ) {
    }

    public function contributors() : array
    {
        if ($this->parameters->get('kernel.environment') === 'test') {
            return [
                [
                    'login' => 'norberttech',
                    'avatar_url' => 'https://avatars.githubusercontent.com/u/1921950?v=4&s=128',
                    'html_url' => 'https://github.com/norberttech',
                ],
            ];
        }

        $factory = new Psr17Factory();
        $client = new PSR18TraceableClient(new Client($factory, $factory), $this->telemetry);

        $from_github = new PsrHttpClientDynamicExtractor($client, $this->requestFactory);

        try {
            df(
                config_builder()
                    ->cache($this->cache('flow-github-contributors'))
                    ->withTelemetry(
                        $this->telemetry,
                        telemetry_options()
                            ->collectMetrics()
                            ->traceLoading()
                            ->traceTransformations()
                            ->filesystem(filesystem_telemetry_options()->collectMetrics()->traceStreams())
                    )
            )
                ->read(
                    from_cache(
                        'flow_github_contributors',
                        $from_github
                    )
                )
                ->cache('flow_github_contributors')
                ->withEntry('unpacked', ref('response_body')->jsonDecode())
                ->select('unpacked')
                ->withEntry('data', ref('unpacked')->expand())
                ->withEntry('data', ref('data')->unpack())
                ->renameEach(rename_replace('data.', ''))
                ->drop('unpacked', 'data')
                ->filter(not(ref('login')->endsWith(lit('[bot]'))))
                ->filter(not(ref('login')->equals(lit('aeon-automation'))))
                ->filter(not(ref('login')->equals(lit('norbertmwk'))))
                ->withEntry('avatar_url', ref('avatar_url')->concat(lit('&s=128')))
                ->limit(24)
                ->write(to_memory($memory = new ArrayMemory()))
                ->run();

            return $memory->dump();
        } catch (\Exception) {
            return [];
        }
    }

    private function cache(string $directoryName) : PSRSimpleCache
    {
        return new PSRSimpleCache(
            new Psr16Cache(
                new TraceableCacheAdapter(
                    new FilesystemAdapter(
                        'flow-website',
                        3600 * 24,
                        directory: $this->parameters->get('kernel.cache_dir') . '/' . \ltrim($directoryName, '/')
                    ),
                    $this->telemetry,
                    'flow-website'
                )
            )
        );
    }
}
