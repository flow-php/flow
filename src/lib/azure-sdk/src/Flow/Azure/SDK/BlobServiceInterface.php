<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\CopyBlob\CopyBlobOptions;
use Flow\Azure\SDK\BlobService\CreateContainer\CreateContainerOptions;
use Flow\Azure\SDK\BlobService\DeleteBlob\DeleteBlobOptions;
use Flow\Azure\SDK\BlobService\DeleteContainer\DeleteContainerOptions;
use Flow\Azure\SDK\BlobService\GetBlob\{BlobContent, GetBlobOptions};
use Flow\Azure\SDK\BlobService\GetBlobProperties\{BlobProperties, GetBlobPropertiesOptions};
use Flow\Azure\SDK\BlobService\GetBlockBlobBlockList\GetBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobService\GetContainerProperties\{ContainerProperties, GetContainerPropertiesOptions};
use Flow\Azure\SDK\BlobService\ListBlobs\{Blob, ListBlobOptions};
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\{PutBlockBlobBlockListOptions, SimpleXMLSerializer};

interface BlobServiceInterface
{
    public function copyBlob(string $fromBlob, string $toBlob, CopyBlobOptions $options = new CopyBlobOptions()) : void;

    public function deleteBlob(string $blob, DeleteBlobOptions $options = new DeleteBlobOptions()) : void;

    public function deleteContainer(DeleteContainerOptions $options = new DeleteContainerOptions()) : void;

    public function getBlob(string $blob, GetBlobOptions $options = new GetBlobOptions()) : BlobContent;

    public function getBlobProperties(string $blob, GetBlobPropertiesOptions $options = new GetBlobPropertiesOptions()) : ?BlobProperties;

    public function getBlockBlobBlockList(string $blob, GetBlockBlobBlockListOptions $options = new GetBlockBlobBlockListOptions()) : BlockList;

    public function getContainerProperties(GetContainerPropertiesOptions $options = new GetContainerPropertiesOptions()) : ?ContainerProperties;

    /**
     * @return \Generator<Blob>
     */
    public function listBlobs(ListBlobOptions $options = new ListBlobOptions()) : \Generator;

    /**
     * @param null|resource|string $content
     */
    public function putBlockBlob(string $path, $content = null, ?int $size = null, PutBlockBlobOptions $options = new PutBlockBlobOptions()) : void;

    /**
     * @param resource|string $content
     */
    public function putBlockBlobBlock(string $path, string $blockId, $content, int $size, PutBlockBlobBlockOptions $options = new PutBlockBlobBlockOptions()) : void;

    public function putBlockBlobBlockList(string $path, BlockList $blockList, PutBlockBlobBlockListOptions $options = new PutBlockBlobBlockListOptions(), Serializer $serializer = new SimpleXMLSerializer()) : void;

    public function putContainer(CreateContainerOptions $options = new CreateContainerOptions()) : void;
}
