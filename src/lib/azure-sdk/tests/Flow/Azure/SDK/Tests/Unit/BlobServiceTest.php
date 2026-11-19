<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockState;
use Flow\Azure\SDK\BlobService\ListBlobs\Blob;
use Flow\Azure\SDK\Exception\AzureException;
use Flow\Azure\SDK\Exception\InvalidArgumentException;
use Flow\Azure\SDK\Tests\Context\BlobServiceContext;
use Flow\Azure\SDK\Tests\Double\FixedResponseClient;
use PHPUnit\Framework\TestCase;

use function array_fill;
use function array_map;
use function range;
use function substr_count;

final class BlobServiceTest extends TestCase
{
    public function test_delete_blobs_counts_a_missing_blob_as_deleted(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::batchResponse([0 => 202, 1 => 404]));

        BlobServiceContext::serviceWithClient($client)->deleteBlobs(['a.txt', 'b.txt']);

        static::assertCount(1, $client->requests());
    }

    public function test_delete_blobs_reads_parts_out_of_order(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::batchResponse([2 => 202, 0 => 202, 1 => 202]));

        BlobServiceContext::serviceWithClient($client)->deleteBlobs(['a.txt', 'b.txt', 'c.txt']);

        static::assertCount(1, $client->requests());
    }

    public function test_delete_blobs_refuses_more_than_256_blobs(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Blob Batch accepts at most 256 blobs, got 257');

        BlobServiceContext::service()->deleteBlobs(array_map(
            static fn(int $index): string => "{$index}.txt",
            range(0, 256),
        ));
    }

    public function test_delete_blobs_reports_every_failed_part_with_the_first_failed_subrequest(): void
    {
        try {
            BlobServiceContext::service(BlobServiceContext::batchResponse([
                0 => 202,
                1 => 403,
                2 => 500,
            ]))->deleteBlobs([
                'a.txt',
                'b.txt',
                'c.txt',
            ]);

            static::fail('deleteBlobs() with failed parts must throw');
        } catch (AzureException $exception) {
            static::assertSame(2, $exception->failedRequests);
            static::assertSame('/container/b.txt', $exception->request->getUri()->getPath());
            static::assertSame(403, $exception->response->getStatusCode());
            static::assertSame('Code403', $exception->response->getHeaderLine('x-ms-error-code'));
            static::assertStringContainsString('<Code>Code403</Code>', $exception->getMessage());
        }
    }

    public function test_delete_blobs_reports_a_blob_without_a_part_as_failed(): void
    {
        try {
            BlobServiceContext::service(BlobServiceContext::batchResponse([0 => 202]))->deleteBlobs(['a.txt', 'b.txt']);

            static::fail('deleteBlobs() with a missing part must throw');
        } catch (AzureException $exception) {
            static::assertSame(1, $exception->failedRequests);
            static::assertSame('/container/b.txt', $exception->request->getUri()->getPath());
        }
    }

    public function test_delete_blobs_sends_no_request_for_no_blobs(): void
    {
        $client = new FixedResponseClient();

        BlobServiceContext::serviceWithClient($client)->deleteBlobs([]);

        static::assertSame([], $client->requests());
    }

    public function test_delete_blobs_sends_one_batch_with_a_part_per_blob(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::batchResponse(array_fill(0, 2, 202)));

        BlobServiceContext::serviceWithClient($client)->deleteBlobs(['dir/a.txt', 'dir/b.txt']);

        $request = $client->requests()[0];
        $body = (string) $request->getBody();
        static::assertSame('POST', $request->getMethod());
        static::assertSame('restype=container&comp=batch', $request->getUri()->getQuery());
        static::assertSame('2024-08-04', $request->getHeaderLine('x-ms-version'));
        static::assertStringStartsWith('multipart/mixed; boundary=batch_', $request->getHeaderLine('content-type'));
        static::assertSame(2, substr_count($body, "Content-Type: application/http\r\n"));
        static::assertStringContainsString("Content-ID: 0\r\n\r\nDELETE /container/dir/a.txt HTTP/1.1\r\n", $body);
        static::assertStringContainsString("Content-ID: 1\r\n\r\nDELETE /container/dir/b.txt HTTP/1.1\r\n", $body);
        static::assertStringNotContainsString('x-ms-version', $body);
        static::assertSame(2, substr_count($body, 'authorization: SharedKey account:'));
    }

    public function test_delete_blobs_throws_on_a_batch_status_other_than_202(): void
    {
        $this->expectException(AzureException::class);

        BlobServiceContext::service(BlobServiceContext::response(400, ''))->deleteBlobs(['a.txt']);
    }

    public function test_delete_container_addresses_the_container(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::response(202, ''));

        BlobServiceContext::serviceWithClient($client)->deleteContainer();

        static::assertSame('DELETE', $client->requests()[0]->getMethod());
        static::assertStringContainsString('restype=container', $client->requests()[0]->getUri()->getQuery());
    }

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

    public function test_copy_blob_throws_when_the_copy_is_not_finished(): void
    {
        $this->expectException(AzureException::class);

        BlobServiceContext::service(BlobServiceContext::responseWithHeaders(202, [
            'x-ms-copy-status' => 'pending',
        ]))->copyBlob('a', 'b');
    }

    public function test_get_blob_properties_sends_head(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::response(200, ''));

        BlobServiceContext::serviceWithClient($client)->getBlobProperties('a');

        static::assertSame('HEAD', $client->requests()[0]->getMethod());
    }

    public function test_copy_blob_sends_a_synchronous_copy_from_a_signed_source(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::responseWithHeaders(202, [
            'x-ms-copy-status' => 'success',
        ]));

        BlobServiceContext::serviceWithClient($client)->copyBlob('a', 'b');

        $request = $client->requests()[0];
        static::assertSame('PUT', $request->getMethod());
        static::assertSame('https://account.blob.core.windows.net/container/b', (string) $request->getUri());
        static::assertSame('true', $request->getHeaderLine('x-ms-requires-sync'));
        static::assertStringStartsWith(
            'https://account.blob.core.windows.net/container/a?sv=2024-08-04&sr=b&sp=r&se=',
            $request->getHeaderLine('x-ms-copy-source'),
        );
        static::assertStringContainsString('&sig=', $request->getHeaderLine('x-ms-copy-source'));
        static::assertSame('0', $request->getHeaderLine('content-length'));
    }

    public function test_copy_blob_throws_on_a_status_other_than_202(): void
    {
        $this->expectException(AzureException::class);

        BlobServiceContext::service(BlobServiceContext::response(409, ''))->copyBlob('a', 'b');
    }

    public function test_put_block_blob_block_from_blob_copies_a_range(): void
    {
        $client = new FixedResponseClient(BlobServiceContext::response(201, ''));

        BlobServiceContext::serviceWithClient($client)->putBlockBlobBlockFromBlob('b', 'block-1', 'a', 5, 9);

        $request = $client->requests()[0];
        static::assertSame('PUT', $request->getMethod());
        static::assertSame('comp=block&blockid=block-1', $request->getUri()->getQuery());
        static::assertSame('bytes=5-9', $request->getHeaderLine('x-ms-source-range'));
        static::assertStringStartsWith(
            'https://account.blob.core.windows.net/container/a?sv=2024-08-04&sr=b&sp=r&se=',
            $request->getHeaderLine('x-ms-copy-source'),
        );
        static::assertStringContainsString('&sig=', $request->getHeaderLine('x-ms-copy-source'));
        static::assertSame('0', $request->getHeaderLine('content-length'));
    }

    public function test_put_block_blob_block_from_blob_throws_on_a_status_other_than_201(): void
    {
        $this->expectException(AzureException::class);

        BlobServiceContext::service(BlobServiceContext::response(403, ''))->putBlockBlobBlockFromBlob(
            'b',
            'block-1',
            'a',
            5,
            9,
        );
    }
}
