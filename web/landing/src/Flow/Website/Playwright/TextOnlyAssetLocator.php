<?php

declare(strict_types=1);

namespace Flow\Website\Playwright;

use Playwright\Symfony\Client\Interception\AssetFile;
use Playwright\Symfony\Client\Interception\AssetLocatorInterface;

use function mb_strtolower;
use function str_starts_with;

/**
 * Hides binary assets from playwright-symfony's in-process AssetServer.
 *
 * That server corrupts every binary body it serves (see BinarySafeResponseConverter for the trace),
 * and it is `final`, so it cannot be subclassed. Its locators can be replaced though: returning null
 * for a binary asset makes `AssetServer::handle()` miss, and PlaywrightKernelClient then falls back
 * to the kernel - where the fixed converter serves the bytes correctly.
 *
 * Text assets keep the fast in-process path. Remove this class once upstream is fixed.
 */
final readonly class TextOnlyAssetLocator implements AssetLocatorInterface
{
    // the complement of AssetServer::isBinaryContentType(), NOT of ResponseConverter's - the two
    // upstream lists differ, and image/svg+xml is text to one and binary to the other
    private const TEXT = [
        'text/',
        'application/json',
        'application/javascript',
        'application/xml',
        'application/xhtml+xml',
    ];

    public function __construct(
        private AssetLocatorInterface $inner,
    ) {}

    public function locate(string $requestPath): ?AssetFile
    {
        $asset = $this->inner->locate($requestPath);

        return $asset !== null && $this->isText($asset->getContentType()) ? $asset : null;
    }

    private function isText(string $contentType): bool
    {
        $contentType = mb_strtolower($contentType);

        foreach (self::TEXT as $prefix) {
            if (str_starts_with($contentType, $prefix)) {
                return true;
            }
        }

        return false;
    }
}
