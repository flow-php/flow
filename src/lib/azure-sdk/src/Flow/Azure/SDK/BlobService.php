<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use DateTimeImmutable;
use Flow\Azure\SDK\BlobService\BlockBlob\Block;
use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\BlockBlob\BlockState;
use Flow\Azure\SDK\BlobService\Configuration;
use Flow\Azure\SDK\BlobService\CopyBlob\CopyBlobOptions;
use Flow\Azure\SDK\BlobService\CreateContainer\CreateContainerOptions;
use Flow\Azure\SDK\BlobService\DeleteBlob\DeleteBlobOptions;
use Flow\Azure\SDK\BlobService\DeleteBlobs\DeleteBlobsOptions;
use Flow\Azure\SDK\BlobService\DeleteContainer\DeleteContainerOptions;
use Flow\Azure\SDK\BlobService\GetBlob\BlobContent;
use Flow\Azure\SDK\BlobService\GetBlob\GetBlobOptions;
use Flow\Azure\SDK\BlobService\GetBlobProperties\BlobProperties;
use Flow\Azure\SDK\BlobService\GetBlobProperties\GetBlobPropertiesOptions;
use Flow\Azure\SDK\BlobService\GetBlockBlobBlockList\GetBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobService\GetContainerProperties\ContainerProperties;
use Flow\Azure\SDK\BlobService\GetContainerProperties\GetContainerPropertiesOptions;
use Flow\Azure\SDK\BlobService\ListBlobs\Blob;
use Flow\Azure\SDK\BlobService\ListBlobs\ListBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\SimpleXMLSerializer;
use Flow\Azure\SDK\Exception\AzureException;
use Flow\Azure\SDK\Exception\InvalidArgumentException;
use Flow\Azure\SDK\Normalizer\SimpleXMLNormalizer;
use Generator;
use Psr\Http\Client\ClientExceptionInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

use function array_key_exists;
use function array_keys;
use function array_merge;
use function array_values;
use function bin2hex;
use function count;
use function explode;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function gmdate;
use function implode;
use function in_array;
use function is_array;
use function is_resource;
use function is_string;
use function ltrim;
use function preg_match;
use function random_bytes;
use function rtrim;
use function strlen;

final readonly class BlobService implements BlobServiceInterface
{
    public const string VERSION = '2024-08-04';

    public function __construct(
        private Configuration $configuration,
        private ClientInterface $httpClient,
        private HttpFactory $httpFactory,
        private URLFactory $urlFactory,
        private AuthorizationFactory $authorizationFactory,
        private LoggerInterface $logger,
    ) {}

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function copyBlob(string $fromBlob, string $toBlob, CopyBlobOptions $options = new CopyBlobOptions()): void
    {
        $request = $this->httpFactory->put($this->urlFactory->create(
            $this->configuration,
            $toBlob,
            $options->toURIParameters(),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $request = $request->withHeader('x-ms-requires-sync', 'true');

        $this->logger->info('Azure - Blob Service - Copy Blob From URL', ['request' => $request]);

        $request = $request->withHeader('x-ms-copy-source', $this->authorizationFactory->readOnlySourceUrl(
            $this->urlFactory->create($this->configuration, $fromBlob),
            $this->configuration->container,
            $fromBlob,
            new DateTimeImmutable('+15 minutes'),
        ));
        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));
        $request = $request->withHeader('content-length', '0');

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Copy Blob From URL', ['response' => $response]);

        if ($response->getStatusCode() !== 202 || $response->getHeaderLine('x-ms-copy-status') !== 'success') {
            $this->logger->critical('Azure - Blob Service - Copy Blob From URL', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function deleteBlob(string $blob, DeleteBlobOptions $options = new DeleteBlobOptions()): void
    {
        $request = $this->httpFactory->delete($this->urlFactory->create(
            $this->configuration,
            $blob,
            $options->toURIParameters(),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Delete Blob', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Delete Blob', ['response' => $response]);

        if ($response->getStatusCode() !== 202) {
            $this->logger->critical('Azure - Blob Service - Delete Blob', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @param list<string> $blobs at most 256, all in the configured container
     *
     * @throws AzureException
     * @throws ClientExceptionInterface
     * @throws InvalidArgumentException
     */
    public function deleteBlobs(array $blobs, DeleteBlobsOptions $options = new DeleteBlobsOptions()): void
    {
        if ($blobs === []) {
            return;
        }

        count($blobs) <= 256
            || throw new InvalidArgumentException('Blob Batch accepts at most 256 blobs, got ' . count($blobs));

        $boundary = 'batch_' . bin2hex(random_bytes(16));
        $date = gmdate('D, d M Y H:i:s T', time());
        $subrequests = [];
        $body = '';

        foreach (array_values($blobs) as $index => $blob) {
            $subrequest = $this->httpFactory
                ->delete($this->urlFactory->create($this->configuration, $blob))
                ->withHeader('date', $date)
                ->withHeader('content-length', '0');

            foreach ($options->toSubrequestHeaders() as $header => $value) {
                $subrequest = $subrequest->withHeader($header, $value);
            }

            $subrequests[$index] = $subrequest->withHeader(
                'authorization',
                $this->authorizationFactory->for($subrequest),
            );

            $body .=
                "--{$boundary}\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n"
                . "Content-ID: {$index}\r\n\r\nDELETE {$subrequests[$index]->getUri()->getPath()} HTTP/1.1\r\n";

            foreach ($subrequests[$index]->getHeaders() as $header => $values) {
                $body .= $header . ': ' . implode(', ', $values) . "\r\n";
            }

            $body .= "\r\n";
        }

        $body .= "--{$boundary}--\r\n";

        $request = $this->httpFactory
            ->post($this->urlFactory->create(
                $this->configuration,
                queryParameters: array_merge($options->toURIParameters(), [
                    'restype' => 'container',
                    'comp' => 'batch',
                ]),
            ))
            ->withHeader('date', $date)
            ->withHeader('content-type', 'multipart/mixed; boundary=' . $boundary)
            ->withHeader('content-length', (string) strlen($body))
            ->withBody($this->httpFactory->stream($body));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Blob Batch', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Blob Batch', ['response' => $response]);

        if ($response->getStatusCode() !== 202) {
            $this->logger->critical('Azure - Blob Service - Blob Batch', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }

        $results = $this->blobBatchResults($response);
        $failures = [];

        foreach (array_keys($subrequests) as $index) {
            if (!array_key_exists($index, $results) || !in_array($results[$index][0], [202, 404], true)) {
                $failures[] = $index;
            }
        }

        if ($failures === []) {
            return;
        }

        $this->logger->critical('Azure - Blob Service - Blob Batch', ['response' => $response]);

        if (!array_key_exists($failures[0], $results)) {
            throw new AzureException(__METHOD__, $subrequests[$failures[0]], $response, count($failures));
        }

        [$status, $errorCode, $partBody] = $results[$failures[0]];

        throw new AzureException(
            __METHOD__,
            $subrequests[$failures[0]],
            $response
                ->withStatus($status)
                ->withHeader('x-ms-error-code', $errorCode)
                ->withBody($this->httpFactory->stream($partBody)),
            count($failures),
        );
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function deleteContainer(DeleteContainerOptions $options = new DeleteContainerOptions()): void
    {
        $request = $this->httpFactory->delete($this->urlFactory->create(
            $this->configuration,
            null,
            array_merge($options->toURIParameters(), ['restype' => 'container']),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Delete Container', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Delete Container', ['response' => $response]);

        if ($response->getStatusCode() !== 202) {
            $this->logger->critical('Azure - Blob Service - Delete Container', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function getBlob(string $blob, GetBlobOptions $options = new GetBlobOptions()): BlobContent
    {
        $request = $this->httpFactory->get($this->urlFactory->create(
            $this->configuration,
            $blob,
            $options->toURIParameters(),
        ));

        $request = $request->withHeader('content-type', 'application/x-www-form-urlencoded');
        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Get Blob', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Get Blob', ['response' => $response]);

        if ($response->getStatusCode() < 200 || $response->getStatusCode() >= 300) {
            $this->logger->critical('Azure - Blob Service - Get Blob', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }

        return new BlobContent($response);
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function getBlobProperties(
        string $blob,
        GetBlobPropertiesOptions $options = new GetBlobPropertiesOptions(),
    ): ?BlobProperties {
        $request = $this->httpFactory->head($this->urlFactory->create(
            $this->configuration,
            $blob,
            $options->toURIParameters(),
        ));

        $request = $request->withHeader('content-type', 'application/x-www-form-urlencoded');
        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Get Blob Properties', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Get Blob Properties', ['response' => $response]);

        if ($response->getStatusCode() === 404) {
            return null;
        }

        if ($response->getStatusCode() < 200 || $response->getStatusCode() >= 300) {
            $this->logger->critical('Azure - Blob Service - Get Blob Properties', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }

        return new BlobProperties($response);
    }

    public function getBlockBlobBlockList(
        string $blob,
        GetBlockBlobBlockListOptions $options = new GetBlockBlobBlockListOptions(),
    ): BlockList {
        $request = $this->httpFactory->get($this->urlFactory->create(
            $this->configuration,
            $blob,
            array_merge($options->toURIParameters(), ['comp' => 'blocklist']),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Get Block Blob Block List', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Get Block Blob Block List', ['response' => $response]);

        if ($response->getStatusCode() !== 200) {
            $this->logger->critical('Azure - Blob Service - Get Block Blob Block List', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }

        $normalized = (new SimpleXMLNormalizer())->toArray($response->getBody()->getContents());

        $blocks = [];

        if (array_key_exists('CommittedBlocks', $normalized) && is_array($normalized['CommittedBlocks'])) {
            $committedBlocks = type_array()->assert($normalized['CommittedBlocks']);

            if (array_key_exists('Block', $committedBlocks) && is_array($committedBlocks['Block'])) {
                $blockData = type_array()->assert($committedBlocks['Block']);

                if (array_key_exists('Name', $blockData)) {
                    $blocks[] = new Block(
                        type_string()->assert($blockData['Name']),
                        BlockState::COMMITTED,
                        (int) type_union(type_integer(), type_string())->assert($blockData['Size']),
                    );
                } else {
                    foreach (type_list(type_array())->assert($blockData) as $blockEntry) {
                        $blocks[] = new Block(
                            type_string()->assert($blockEntry['Name']),
                            BlockState::COMMITTED,
                            (int) type_union(type_integer(), type_string())->assert($blockEntry['Size']),
                        );
                    }
                }
            }
        }

        if (array_key_exists('UncommittedBlocks', $normalized) && is_array($normalized['UncommittedBlocks'])) {
            $uncommittedBlocks = type_array()->assert($normalized['UncommittedBlocks']);

            if (array_key_exists('Block', $uncommittedBlocks) && is_array($uncommittedBlocks['Block'])) {
                $blockData = type_array()->assert($uncommittedBlocks['Block']);

                if (array_key_exists('Name', $blockData)) {
                    $blocks[] = new Block(
                        type_string()->assert($blockData['Name']),
                        BlockState::UNCOMMITTED,
                        (int) type_union(type_integer(), type_string())->assert($blockData['Size']),
                    );
                } else {
                    foreach (type_list(type_array())->assert($blockData) as $blockEntry) {
                        $blocks[] = new Block(
                            type_string()->assert($blockEntry['Name']),
                            BlockState::UNCOMMITTED,
                            (int) type_union(type_integer(), type_string())->assert($blockEntry['Size']),
                        );
                    }
                }
            }
        }

        return new BlockList(...$blocks);
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function getContainerProperties(
        GetContainerPropertiesOptions $options = new GetContainerPropertiesOptions(),
    ): ?ContainerProperties {
        $request = $this->httpFactory->get($this->urlFactory->create(
            $this->configuration,
            null,
            array_merge($options->toURIParameters(), ['restype' => 'container']),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Get Container Properties', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Get Container Properties', ['response' => $response]);

        if ($response->getStatusCode() === 404) {
            return null;
        }

        if ($response->getStatusCode() < 200 || $response->getStatusCode() >= 300) {
            $this->logger->critical('Azure - Blob Service - Get Container Properties', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }

        return new ContainerProperties($response);
    }

    /**
     * @throws AzureException
     *
     * @return \Generator<int, Blob>
     */
    public function listBlobs(ListBlobOptions $options = new ListBlobOptions()): Generator
    {
        $request = $this->httpFactory->get($this->urlFactory->create(
            $this->configuration,
            queryParameters: array_merge($options->toURIParameters(), ['restype' => 'container', 'comp' => 'list']),
        ));

        $request = $request->withHeader('content-type', 'application/x-www-form-urlencoded');
        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - List Blobs', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - List Blobs', ['response' => $response]);

        if ($response->getStatusCode() !== 200) {
            $this->logger->critical('Azure - Blob Service - List Blobs', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }

        $normalized = (new SimpleXMLNormalizer())->toArray($response->getBody()->getContents());

        if (array_key_exists('Blobs', $normalized) && is_array($normalized['Blobs'])) {
            $blobsData = type_array()->assert($normalized['Blobs']);

            if (array_key_exists('Blob', $blobsData) && is_array($blobsData['Blob'])) {
                $blobData = type_array()->assert($blobsData['Blob']);

                if (array_key_exists('Name', $blobData)) {
                    yield new Blob($blobData);
                } else {
                    foreach (type_list(type_array())->assert($blobData) as $blobEntry) {
                        yield new Blob($blobEntry);
                    }
                }
            }
        }

        if (array_key_exists('NextMarker', $normalized) && is_string($normalized['NextMarker'])) {
            yield from $this->listBlobs($options->withMarker(type_string()->assert($normalized['NextMarker'])));
        }
    }

    /**
     * @param null|resource|string $content
     *
     * @throws AzureException
     */
    public function putBlockBlob(
        string $path,
        $content = null,
        ?int $size = null,
        PutBlockBlobOptions $options = new PutBlockBlobOptions(),
    ): void {
        if ($content !== null) {
            if (!is_resource($content) && !is_string($content)) {
                throw new InvalidArgumentException('Content must be a resource or a string');
            }

            if ($size === null) {
                throw new InvalidArgumentException('Size must be provided when content is provided');
            }
        }

        $request = $this->httpFactory->put($this->urlFactory->create(
            $this->configuration,
            $path,
            $options->toURIParameters(),
        ));

        $request = $request
            ->withHeader('content-type', 'application/octet-stream')
            ->withHeader('x-ms-blob-content-type', 'application/octet-stream')
            ->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        if ($content) {
            $request = $request
                ->withHeader('content-length', (string) $size)
                ->withBody($this->httpFactory->stream($content));
        }

        $this->logger->info('Azure - Blob Service - Put Blob Block', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        if (!$content) {
            $request = $request->withHeader('content-length', '0');
        }

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Put Blob Block', ['response' => $response]);

        if ($response->getStatusCode() !== 201) {
            $this->logger->critical('Azure - Blob Service - Put Blob Block', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @param resource|string $content
     *
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function putBlockBlobBlock(
        string $path,
        string $blockId,
        $content,
        int $size,
        PutBlockBlobBlockOptions $options = new PutBlockBlobBlockOptions(),
    ): void {
        $request = $this->httpFactory->put($this->urlFactory->create(
            $this->configuration,
            $path,
            array_merge($options->toURIParameters(), ['comp' => 'block', 'blockid' => $blockId]),
        ));

        $request = $request
            ->withHeader('content-type', 'application/x-www-form-urlencoded')
            ->withHeader('date', gmdate('D, d M Y H:i:s T', time()))
            ->withHeader('content-length', (string) $size);

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $request = $request->withBody($this->httpFactory->stream($content))->withHeader(
            'authorization',
            $this->authorizationFactory->for($request),
        );

        $this->logger->info('Azure - Blob Service - Put Block Blob Block', ['request' => $request]);

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Put Block Blob Block', ['response' => $response]);

        if ($response->getStatusCode() !== 201) {
            $this->logger->critical('Azure - Blob Service - Put Block Blob Block', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function putBlockBlobBlockFromBlob(
        string $path,
        string $blockId,
        string $sourceBlob,
        int $first,
        int $last,
        PutBlockBlobBlockOptions $options = new PutBlockBlobBlockOptions(),
    ): void {
        $request = $this->httpFactory->put($this->urlFactory->create(
            $this->configuration,
            $path,
            array_merge($options->toURIParameters(), ['comp' => 'block', 'blockid' => $blockId]),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $request = $request->withHeader('x-ms-source-range', 'bytes=' . $first . '-' . $last);

        $this->logger->info('Azure - Blob Service - Put Block Blob Block From URL', ['request' => $request]);

        $request = $request->withHeader('x-ms-copy-source', $this->authorizationFactory->readOnlySourceUrl(
            $this->urlFactory->create($this->configuration, $sourceBlob),
            $this->configuration->container,
            $sourceBlob,
            new DateTimeImmutable('+15 minutes'),
        ));
        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));
        $request = $request->withHeader('content-length', '0');

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Put Block Blob Block From URL', ['response' => $response]);

        if ($response->getStatusCode() !== 201) {
            $this->logger->critical('Azure - Blob Service - Put Block Blob Block From URL', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function putBlockBlobBlockList(
        string $path,
        BlockList $blockList,
        PutBlockBlobBlockListOptions $options = new PutBlockBlobBlockListOptions(),
        Serializer $serializer = new SimpleXMLSerializer(),
    ): void {
        $request = $this->httpFactory->put($this->urlFactory->create(
            $this->configuration,
            $path,
            queryParameters: array_merge($options->toURIParameters(), ['comp' => 'blocklist']),
        ));

        $request = $request->withHeader('content-type', 'application/x-www-form-urlencoded')->withHeader('date', gmdate(
            'D, d M Y H:i:s T',
            time(),
        ));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $request = $request->withBody($this->httpFactory->stream(
            $blockListString = $serializer->serialize($blockList),
        ))->withHeader('content-length', (string) strlen($blockListString));

        $this->logger->info('Azure - Blob Service - Put Block Blob Block List', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Put Block Blob Block List', ['response' => $response]);

        if ($response->getStatusCode() !== 201) {
            $this->logger->critical('Azure - Blob Service - Put Block Blob Block List', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @throws AzureException
     * @throws ClientExceptionInterface
     */
    public function putContainer(CreateContainerOptions $options = new CreateContainerOptions()): void
    {
        $request = $this->httpFactory->put($this->urlFactory->create(
            $this->configuration,
            null,
            array_merge($options->toURIParameters(), ['restype' => 'container']),
        ));

        $request = $request->withHeader('date', gmdate('D, d M Y H:i:s T', time()));

        foreach ($options->toHeaders() as $header => $value) {
            $request = $request->withHeader($header, $value);
        }

        $this->logger->info('Azure - Blob Service - Put Container', ['request' => $request]);

        $request = $request->withHeader('authorization', $this->authorizationFactory->for($request));

        $response = $this->httpClient->sendRequest($request);

        $this->logger->info('Azure - Blob Service - Put Container', ['response' => $response]);

        if ($response->getStatusCode() !== 201) {
            $this->logger->critical('Azure - Blob Service - Put Container', ['response' => $response]);

            throw new AzureException(__METHOD__, $request, $response);
        }
    }

    /**
     * @return array<int, array{int, string, string}> status, x-ms-error-code and body of each part, by Content-ID
     */
    private function blobBatchResults(ResponseInterface $response): array
    {
        $boundary = [];

        if (preg_match('/boundary="?([^";]+)"?/i', $response->getHeaderLine('content-type'), $boundary) !== 1) {
            return [];
        }

        $results = [];

        foreach (explode('--' . $boundary[1], $response->getBody()->getContents()) as $part) {
            $sections = explode("\r\n\r\n", ltrim($part, "\r\n"), 3);
            $contentId = [];
            $status = [];
            $errorCode = [];

            if (
                count($sections) < 2
                || preg_match('/^Content-ID:\s*(\d+)/mi', $sections[0], $contentId) !== 1
                || preg_match('/^HTTP\/1\.1 (\d{3})/', $sections[1], $status) !== 1
            ) {
                continue;
            }

            preg_match('/^x-ms-error-code:\s*(\S+)/mi', $sections[1], $errorCode);

            $results[(int) $contentId[1]] = [(int) $status[1], $errorCode[1] ?? '', rtrim($sections[2] ?? '', "\r\n")];
        }

        return $results;
    }
}
