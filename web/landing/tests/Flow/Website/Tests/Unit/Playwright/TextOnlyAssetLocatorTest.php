<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Unit\Playwright;

use Flow\Website\Playwright\TextOnlyAssetLocator;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Playwright\Symfony\Client\Interception\AssetFile;
use Playwright\Symfony\Client\Interception\AssetLocatorInterface;

final class TextOnlyAssetLocatorTest extends TestCase
{
    #[TestWith(['text/css'])]
    #[TestWith(['text/html; charset=UTF-8'])]
    #[TestWith(['application/javascript'])]
    #[TestWith(['application/json'])]
    #[TestWith(['application/xml'])]
    #[TestWith(['TEXT/CSS'])]
    public function test_a_text_asset_keeps_the_fast_in_process_path(string $contentType): void
    {
        static::assertNotNull((new TextOnlyAssetLocator(new FakeLocator($contentType)))->locate('/assets/a'));
    }

    #[TestWith(['application/wasm'])]
    #[TestWith(['font/woff2'])]
    #[TestWith(['image/png'])]
    #[TestWith(['application/octet-stream'])]
    #[TestWith(['image/svg+xml'])]
    public function test_a_binary_asset_is_hidden_so_it_falls_through_to_the_kernel(string $contentType): void
    {
        static::assertNull((new TextOnlyAssetLocator(new FakeLocator($contentType)))->locate('/assets/a'));
    }

    public function test_an_asset_the_inner_locator_cannot_find_stays_not_found(): void
    {
        static::assertNull((new TextOnlyAssetLocator(new FakeLocator(null)))->locate('/assets/missing'));
    }
}

final readonly class FakeLocator implements AssetLocatorInterface
{
    public function __construct(
        private ?string $contentType,
    ) {}

    public function locate(string $requestPath): ?AssetFile
    {
        return $this->contentType === null ? null : new AssetFile(null, null, null, $this->contentType, 'body');
    }
}
