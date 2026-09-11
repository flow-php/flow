<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Double;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\CopyBlob\CopyBlobOptions;
use Flow\Azure\SDK\BlobService\CreateContainer\CreateContainerOptions;
use Flow\Azure\SDK\BlobService\DeleteBlob\DeleteBlobOptions;
use Flow\Azure\SDK\BlobService\DeleteContainer\DeleteContainerOptions;
use Flow\Azure\SDK\BlobService\GetBlob\BlobContent;
use Flow\Azure\SDK\BlobService\GetBlob\GetBlobOptions;
use Flow\Azure\SDK\BlobService\GetBlobProperties\BlobProperties;
use Flow\Azure\SDK\BlobService\GetBlobProperties\GetBlobPropertiesOptions;
use Flow\Azure\SDK\BlobService\GetBlockBlobBlockList\GetBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobService\GetContainerProperties\ContainerProperties;
use Flow\Azure\SDK\BlobService\GetContainerProperties\GetContainerPropertiesOptions;
use Flow\Azure\SDK\BlobService\ListBlobs\ListBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\SimpleXMLSerializer;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Azure\SDK\Serializer;
use Generator;
use LogicException;
use Nyholm\Psr7\Response;
use RuntimeException;

use function preg_match;
use function strlen;
use function substr;

use const PHP_INT_MAX;

final class InMemoryBlobService implements BlobServiceInterface
{
    private int $reads = 0;

    public function __construct(
        private readonly string $content,
        private readonly ?int $reportedSize = null,
        private readonly int $maxReads = PHP_INT_MAX,
    ) {}

    public function copyBlob(
        string $fromBlob,
        string $toBlob,
        CopyBlobOptions $options = new CopyBlobOptions(),
    ): void {}

    public function deleteBlob(string $blob, DeleteBlobOptions $options = new DeleteBlobOptions()): void {}

    public function deleteContainer(DeleteContainerOptions $options = new DeleteContainerOptions()): void {}

    public function getBlob(string $blob, GetBlobOptions $options = new GetBlobOptions()): BlobContent
    {
        if (++$this->reads > $this->maxReads) {
            throw new RuntimeException("short-read double: more than {$this->maxReads} reads, the loop does not stop");
        }

        $matches = [];
        preg_match('/bytes=(\d+)-(\d*)/', $options->toHeaders()['x-ms-range'], $matches);
        $start = (int) $matches[1];

        return new BlobContent(
            new Response(
                206,
                [],
                substr($this->content, $start, $matches[2] === '' ? null : (int) $matches[2] - $start + 1),
            ),
        );
    }

    public function getBlobProperties(
        string $blob,
        GetBlobPropertiesOptions $options = new GetBlobPropertiesOptions(),
    ): ?BlobProperties {
        return new BlobProperties(new Response(200, [
            'Content-Length' => (string) ($this->reportedSize ?? strlen($this->content)),
        ]));
    }

    public function getBlockBlobBlockList(
        string $blob,
        GetBlockBlobBlockListOptions $options = new GetBlockBlobBlockListOptions(),
    ): BlockList {
        throw new LogicException('unused');
    }

    public function getContainerProperties(GetContainerPropertiesOptions $options = new GetContainerPropertiesOptions()): ?ContainerProperties
    {
        return null;
    }

    public function listBlobs(ListBlobOptions $options = new ListBlobOptions()): Generator
    {
        yield from [];
    }

    public function putBlockBlob(
        string $path,
        $content = null,
        ?int $size = null,
        PutBlockBlobOptions $options = new PutBlockBlobOptions(),
    ): void {}

    public function putBlockBlobBlock(
        string $path,
        string $blockId,
        $content,
        int $size,
        PutBlockBlobBlockOptions $options = new PutBlockBlobBlockOptions(),
    ): void {}

    public function putBlockBlobBlockList(
        string $path,
        BlockList $blockList,
        PutBlockBlobBlockListOptions $options = new PutBlockBlobBlockListOptions(),
        Serializer $serializer = new SimpleXMLSerializer(),
    ): void {}

    public function putContainer(CreateContainerOptions $options = new CreateContainerOptions()): void {}
}
