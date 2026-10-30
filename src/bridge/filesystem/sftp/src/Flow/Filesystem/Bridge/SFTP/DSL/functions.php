<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\DSL;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\Filesystem\Bridge\SFTP\ConfiguredSFTPClient;
use Flow\Filesystem\Bridge\SFTP\LiveSFTPClient;
use Flow\Filesystem\Bridge\SFTP\Options;
use Flow\Filesystem\Bridge\SFTP\SFTPFilesystem;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Mount;
use phpseclib4\Crypt\Common\PrivateKey;
use phpseclib4\Net\SFTP;
use SensitiveParameter;

use function sprintf;

/**
 * @throws RuntimeException
 */
#[DocumentationDSL(module: Module::SFTP_FILESYSTEM, type: Type::HELPER)]
function sftp_client(
    string $host,
    string $user,
    #[SensitiveParameter]
    string|PrivateKey $credential,
    int $port = 22,
): SFTP {
    $sftp = new SFTP($host, $port);

    if (!$sftp->login($user, $credential)) {
        throw new RuntimeException(sprintf(
            'Failed to authenticate user "%s" against sftp://%s:%d',
            $user,
            $host,
            $port,
        ));
    }

    return $sftp;
}

/**
 * @param array<array-key, mixed>|SFTP $sftp a configuration array (host, user, port?, password | private_key + passphrase?) builds a client that can be described for another process; a live client stays in-process
 */
#[DocumentationDSL(module: Module::SFTP_FILESYSTEM, type: Type::HELPER)]
function sftp_filesystem(
    #[SensitiveParameter]
    SFTP|array $sftp,
    Options $options = new Options(),
    string $protocol = 'sftp',
): SFTPFilesystem {
    return new SFTPFilesystem(
        new Mount($protocol),
        $sftp instanceof SFTP ? new LiveSFTPClient($sftp) : ConfiguredSFTPClient::fromArray($sftp),
        $options,
    );
}

#[DocumentationDSL(module: Module::SFTP_FILESYSTEM, type: Type::HELPER)]
function sftp_filesystem_options(): Options
{
    return new Options();
}
