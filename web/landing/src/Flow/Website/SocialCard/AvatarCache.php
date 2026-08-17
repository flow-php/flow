<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard;

use Symfony\Contracts\HttpClient\Exception\ExceptionInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;

use function file_exists;
use function file_put_contents;
use function getimagesizefromstring;
use function is_dir;
use function mkdir;
use function sprintf;

final readonly class AvatarCache
{
    public function __construct(
        private HttpClientInterface $httpClient,
        private string $cacheDir,
        private string $urlTemplate = 'https://github.com/%s.png?size=96',
    ) {}

    public function fetch(string $handle): ?string
    {
        $path = $this->cacheDir . '/' . $handle . '.png';

        if (file_exists($path)) {
            return $path;
        }

        try {
            $avatar = $this->httpClient->request('GET', sprintf($this->urlTemplate, $handle), [
                'timeout' => 5,
            ])->getContent();
        } catch (ExceptionInterface) {
            return null;
        }

        if (getimagesizefromstring($avatar) === false) {
            return null;
        }

        if (
            !is_dir($this->cacheDir)
            && !mkdir($this->cacheDir, permissions: 0o755, recursive: true)
            && !is_dir($this->cacheDir)
        ) {
            return null;
        }

        file_put_contents($path, $avatar);

        return $path;
    }
}
