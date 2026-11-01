<?php

declare(strict_types=1);

namespace Flow\Website\Playwright;

use Playwright\Symfony\Client\Interception\AssetFile;
use Playwright\Symfony\Client\Interception\AssetLocatorInterface;

use function mb_strtolower;
use function str_starts_with;

/**
 * Workaround: binary assets bypass the corrupting AssetServer; remove once fixed upstream.
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
