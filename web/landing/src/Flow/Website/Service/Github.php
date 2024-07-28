<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use function Flow\ETL\DSL\{config_builder, df, from_cache, lit, not, ref, to_memory};
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

        $adapter = new PSRSimpleCache($this->cache('flow-github-contributors'));

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
                ->filter(not(ref('login')->equals(lit('norbertmwk'))))
                ->withEntry('avatar_url', ref('avatar_url')->concat(lit('&s=128')))
                ->limit(24)
                ->write(to_memory($memory = new ArrayMemory()))
                ->run();

            return $memory->dump();
        } catch (\Exception $e) {
            return [];
        }
    }

    public function version(string $project) : string
    {
        $cache = $this->cache('flow-github-version');

        if ($cache->has('version')) {
            return $cache->get('version');
        }

        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        $request = $factory
            ->createRequest('GET', 'https://api.github.com/repos/' . $project . '/releases/latest')
            ->withHeader('Accept', 'application/vnd.github+json')
            ->withHeader('Authorization', 'Bearer ' . $this->requestFactory->githubToken)
            ->withHeader('X-GitHub-Api-Version', '2022-11-28')
            ->withHeader('User-Agent', 'flow-website-fetch');

        $response = $client->sendRequest($request);

        if ($response->getStatusCode() !== 200) {
            throw new \RuntimeException('Failed to fetch version from Github: ' . $response->getBody()->getContents());
        }

        $data = \json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);

        $version = $data['tag_name'];

        $cache->set('version', $version);

        return $version;
    }

    private function cache(string $directoryName) : Psr16Cache
    {
        return new Psr16Cache(
            new FilesystemAdapter(
                'flow-website',
                3600 * 24,
                directory: $this->parameters->get('kernel.cache_dir') . '/' . \ltrim($directoryName, '/')
            )
        );
    }
}
