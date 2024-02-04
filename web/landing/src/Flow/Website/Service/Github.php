<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Cache\PSRSimpleCache;
use Flow\ETL\Memory\ArrayMemory;
use Flow\Website\Factory\Github\ContributorsRequestFactory;
use Http\Client\Curl\Client;
use Http\Discovery\Psr17Factory;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

final class Github
{
    public function __construct(
        private readonly ContributorsRequestFactory $requestFactory,
        private readonly ContainerBagInterface $parameters
    ) {
    }

    public function contributors() : array
    {
        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        $adapter = new PSRSimpleCache(
            new Psr16Cache(
                new FilesystemAdapter(
                    'flow-website',
                    3600 * 24,
                    directory: $this->parameters->get('kernel.cache_dir') . '/flow-github-contributors'
                )
            )
        );

        $from_github = new PsrHttpClientDynamicExtractor($client, $this->requestFactory);

        try {
            df(config_builder()->cache($adapter))
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
                ->renameAll('data.', '')
                ->drop('unpacked', 'data')
                ->filter(not(ref('login')->endsWith(lit('[bot]'))))
                ->filter(not(ref('login')->equals(lit('aeon-automation'))))
                ->withEntry('avatar_url', ref('avatar_url')->concat(lit('&s=128')))
                ->limit(24)
                ->write(to_memory($memory = new ArrayMemory()))
                ->run();

            return $memory->dump();
        } catch (\Exception $e) {
            return [];
        }
    }
}
