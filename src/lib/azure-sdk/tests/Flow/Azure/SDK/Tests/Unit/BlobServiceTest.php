<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockState;
use Flow\Azure\SDK\BlobService\ListBlobs\Blob;
use Flow\Azure\SDK\Tests\Context\BlobServiceContext;
use Flow\Azure\SDK\Tests\Double\FixedResponseClient;
use PHPUnit\Framework\TestCase;

final class BlobServiceTest extends TestCase
{
    public function test_get_block_blob_block_list_parses_single_committed_block(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <BlockList>
                <CommittedBlocks>
                    <Block>
                        <Name>block-1</Name>
                        <Size>1024</Size>
                    </Block>
                </CommittedBlocks>
                <UncommittedBlocks/>
            </BlockList>
            XML;

        $blocks = BlobServiceContext::service(BlobServiceContext::response(200, $xml))->getBlockBlobBlockList(
            'blob.bin',
        )->all();

        static::assertCount(1, $blocks);
        static::assertSame('block-1', $blocks[0]->id);
        static::assertSame(BlockState::COMMITTED, $blocks[0]->state);
        static::assertSame(1024, $blocks[0]->size);
    }

    public function test_get_block_blob_block_list_parses_multiple_committed_blocks(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <BlockList>
                <CommittedBlocks>
                    <Block>
                        <Name>a</Name>
                        <Size>10</Size>
                    </Block>
                    <Block>
                        <Name>b</Name>
                        <Size>20</Size>
                    </Block>
                </CommittedBlocks>
                <UncommittedBlocks/>
            </BlockList>
            XML;

        $blocks = BlobServiceContext::service(BlobServiceContext::response(200, $xml))->getBlockBlobBlockList(
            'blob.bin',
        )->all();

        static::assertCount(2, $blocks);
        static::assertSame('a', $blocks[0]->id);
        static::assertSame(10, $blocks[0]->size);
        static::assertSame('b', $blocks[1]->id);
        static::assertSame(20, $blocks[1]->size);
    }

    public function test_get_block_blob_block_list_parses_mixed_committed_and_uncommitted_blocks(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <BlockList>
                <CommittedBlocks>
                    <Block>
                        <Name>committed-1</Name>
                        <Size>100</Size>
                    </Block>
                </CommittedBlocks>
                <UncommittedBlocks>
                    <Block>
                        <Name>uncommitted-1</Name>
                        <Size>200</Size>
                    </Block>
                    <Block>
                        <Name>uncommitted-2</Name>
                        <Size>300</Size>
                    </Block>
                </UncommittedBlocks>
            </BlockList>
            XML;

        $blocks = BlobServiceContext::service(BlobServiceContext::response(200, $xml))->getBlockBlobBlockList(
            'blob.bin',
        )->all();

        static::assertCount(3, $blocks);
        static::assertSame('committed-1', $blocks[0]->id);
        static::assertSame(BlockState::COMMITTED, $blocks[0]->state);
        static::assertSame('uncommitted-1', $blocks[1]->id);
        static::assertSame(BlockState::UNCOMMITTED, $blocks[1]->state);
        static::assertSame('uncommitted-2', $blocks[2]->id);
        static::assertSame(BlockState::UNCOMMITTED, $blocks[2]->state);
    }

    public function test_get_block_blob_block_list_returns_empty_when_no_blocks(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <BlockList>
                <CommittedBlocks/>
                <UncommittedBlocks/>
            </BlockList>
            XML;

        static::assertSame(
            [],
            BlobServiceContext::service(BlobServiceContext::response(200, $xml))->getBlockBlobBlockList(
                'blob.bin',
            )->all(),
        );
    }

    public function test_list_blobs_yields_single_blob(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
                <Blobs>
                    <Blob>
                        <Name>file.txt</Name>
                        <Properties>
                            <Content-Length>42</Content-Length>
                        </Properties>
                    </Blob>
                </Blobs>
                <NextMarker/>
            </EnumerationResults>
            XML;

        $blobs = iterator_to_array(
            BlobServiceContext::service(BlobServiceContext::response(200, $xml))->listBlobs(),
            false,
        );

        static::assertCount(1, $blobs);
        static::assertInstanceOf(Blob::class, $blobs[0]);
        static::assertSame('file.txt', $blobs[0]->name());
        static::assertSame(42, $blobs[0]->size());
    }

    public function test_list_blobs_yields_multiple_blobs(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
                <Blobs>
                    <Blob>
                        <Name>a.txt</Name>
                        <Properties>
                            <Content-Length>1</Content-Length>
                        </Properties>
                    </Blob>
                    <Blob>
                        <Name>b.txt</Name>
                        <Properties>
                            <Content-Length>2</Content-Length>
                        </Properties>
                    </Blob>
                </Blobs>
                <NextMarker/>
            </EnumerationResults>
            XML;

        $blobs = iterator_to_array(
            BlobServiceContext::service(BlobServiceContext::response(200, $xml))->listBlobs(),
            false,
        );

        static::assertCount(2, $blobs);
        static::assertSame('a.txt', $blobs[0]->name());
        static::assertSame('b.txt', $blobs[1]->name());
    }

    public function test_list_blobs_yields_nothing_when_blobs_empty(): void
    {
        $xml = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
                <Blobs/>
                <NextMarker/>
            </EnumerationResults>
            XML;

        static::assertSame(
            [],
            iterator_to_array(BlobServiceContext::service(BlobServiceContext::response(200, $xml))->listBlobs(), false),
        );
    }

    public function test_list_blobs_follows_next_marker_pagination(): void
    {
        $firstPage = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
                <Blobs>
                    <Blob>
                        <Name>page1.txt</Name>
                        <Properties>
                            <Content-Length>1</Content-Length>
                        </Properties>
                    </Blob>
                </Blobs>
                <NextMarker>next-token</NextMarker>
            </EnumerationResults>
            XML;

        $secondPage = <<<'XML'
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults>
                <Blobs>
                    <Blob>
                        <Name>page2.txt</Name>
                        <Properties>
                            <Content-Length>2</Content-Length>
                        </Properties>
                    </Blob>
                </Blobs>
                <NextMarker/>
            </EnumerationResults>
            XML;

        $client = new FixedResponseClient(
            BlobServiceContext::response(200, $firstPage),
            BlobServiceContext::response(200, $secondPage),
        );

        $blobs = iterator_to_array(BlobServiceContext::serviceWithClient($client)->listBlobs(), false);

        static::assertCount(2, $blobs);
        static::assertSame('page1.txt', $blobs[0]->name());
        static::assertSame('page2.txt', $blobs[1]->name());

        $sentRequests = $client->requests();
        static::assertCount(2, $sentRequests);
        static::assertStringNotContainsString('marker=', $sentRequests[0]->getUri()->getQuery());
        static::assertStringContainsString('marker=next-token', $sentRequests[1]->getUri()->getQuery());
    }
}
