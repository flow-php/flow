<?php

declare(strict_types=1);

namespace Flow\Website\SocialCard;

use GdImage;
use RuntimeException;

use function count;
use function imagecolorallocatealpha;
use function imagecopyresampled;
use function imagecreatefrompng;
use function imagecreatetruecolor;
use function imageline;
use function imagesx;
use function imagesy;
use function imagettftext;
use function intdiv;
use function round;

final readonly class CardPainter
{
    public const int WIDTH = 1200;

    public const int HEIGHT = 630;

    private const int MARGIN = 72;

    private const int CONTENT_WIDTH = 640;

    private const array BACKGROUND_TOP = [0x26, 0x22, 0x38];

    private const array BACKGROUND_BOTTOM = [0x14, 0x12, 0x1C];

    public const array ORANGE = [0xFF, 0x55, 0x47];

    private const array BLUE = [0x80, 0x6D, 0xFE];

    private const array WHITE = [0xFF, 0xFF, 0xFF];

    private const array MUTED = [0xA9, 0xB1, 0xC2];

    public function __construct(
        private string $resourcesDir,
    ) {}

    public function canvas(): GdImage
    {
        $canvas = imagecreatetruecolor(self::WIDTH, self::HEIGHT);

        if ($canvas === false) {
            throw new RuntimeException('Failed to create image canvas');
        }

        for ($y = 0; $y < self::HEIGHT; $y++) {
            $background = $this->color($canvas, [
                (int) round(
                    self::BACKGROUND_TOP[0]
                    + (($y / self::HEIGHT) * (self::BACKGROUND_BOTTOM[0] - self::BACKGROUND_TOP[0])),
                ),
                (int) round(
                    self::BACKGROUND_TOP[1]
                    + (($y / self::HEIGHT) * (self::BACKGROUND_BOTTOM[1] - self::BACKGROUND_TOP[1])),
                ),
                (int) round(
                    self::BACKGROUND_TOP[2]
                    + (($y / self::HEIGHT) * (self::BACKGROUND_BOTTOM[2] - self::BACKGROUND_TOP[2])),
                ),
            ]);
            imageline($canvas, x1: 0, y1: $y, x2: self::WIDTH - 1, y2: $y, color: $background);
        }

        return $canvas;
    }

    /**
     * @param array{int, int, int} $rgb
     */
    public function color(GdImage $image, array $rgb, int $alpha = 0): int
    {
        $color = imagecolorallocatealpha($image, $rgb[0], $rgb[1], $rgb[2], $alpha);

        if ($color === false) {
            throw new RuntimeException('Failed to allocate color');
        }

        return $color;
    }

    public function drawAccentBar(GdImage $canvas): void
    {
        for ($x = 0; $x < self::WIDTH; $x++) {
            $gradient = $this->color($canvas, [
                (int) round(self::ORANGE[0] + (($x / self::WIDTH) * (self::BLUE[0] - self::ORANGE[0]))),
                (int) round(self::ORANGE[1] + (($x / self::WIDTH) * (self::BLUE[1] - self::ORANGE[1]))),
                (int) round(self::ORANGE[2] + (($x / self::WIDTH) * (self::BLUE[2] - self::ORANGE[2]))),
            ]);
            imageline($canvas, x1: $x, y1: self::HEIGHT - 8, x2: $x, y2: self::HEIGHT - 1, color: $gradient);
        }
    }

    public function drawDescription(GdImage $canvas, string $description, int $baseline): void
    {
        $font = $this->resourcesDir . '/fonts/Cabin-Regular.ttf';

        foreach ((new TextWrapper($font))->wrap(
            $description,
            fontSize: 19,
            maxWidth: self::CONTENT_WIDTH,
            maxLines: 3,
        ) as $line) {
            imagettftext(
                $canvas,
                size: 19,
                angle: 0,
                x: self::MARGIN,
                y: $baseline,
                color: $this->color($canvas, self::MUTED),
                font_filename: $font,
                text: $line,
            );
            $baseline += 38;
        }
    }

    public function drawElephant(GdImage $canvas): void
    {
        $elephant = imagecreatefrompng($this->resourcesDir . '/images/elephant.png');

        if ($elephant === false) {
            throw new RuntimeException('Failed to load elephant image');
        }

        $width = 470;
        $height = (int) round(($width * imagesy($elephant)) / imagesx($elephant));
        imagecopyresampled(
            $canvas,
            $elephant,
            dst_x: 770,
            dst_y: intdiv(self::HEIGHT - $height, num2: 2),
            src_x: 0,
            src_y: 0,
            dst_width: $width,
            dst_height: $height,
            src_width: imagesx($elephant),
            src_height: imagesy($elephant),
        );
    }

    public function drawEyebrow(GdImage $canvas, string $section): void
    {
        $fontBold = $this->resourcesDir . '/fonts/Cabin-Bold.ttf';
        $fontRegular = $this->resourcesDir . '/fonts/Cabin-Regular.ttf';

        $eyebrow = 'FLOW PHP';
        imagettftext(
            $canvas,
            size: 18,
            angle: 0,
            x: self::MARGIN,
            y: 116,
            color: $this->color($canvas, self::ORANGE),
            font_filename: $fontBold,
            text: $eyebrow,
        );

        $eyebrowWidth = (new TextWrapper($fontBold))->width($eyebrow, fontSize: 18);
        imagettftext(
            $canvas,
            size: 18,
            angle: 0,
            x: self::MARGIN + $eyebrowWidth + 14,
            y: 116,
            color: $this->color($canvas, self::MUTED),
            font_filename: $fontRegular,
            text: '· ' . $section,
        );
    }

    /**
     * @param array{int, int, int} $primaryColor
     */
    public function drawFooter(
        GdImage $canvas,
        string $primary,
        ?string $secondary = null,
        ?string $avatarPath = null,
        array $primaryColor = self::BLUE,
    ): void {
        $fontBold = $this->resourcesDir . '/fonts/Cabin-Bold.ttf';
        $fontRegular = $this->resourcesDir . '/fonts/Cabin-Regular.ttf';

        $x = self::MARGIN;

        if (
            $avatarPath !== null
            && (new CircleAvatar())->draw($canvas, $avatarPath, x: self::MARGIN, y: 523, size: 64)
        ) {
            $x += 64 + 18;
        }

        imagettftext(
            $canvas,
            size: 17,
            angle: 0,
            x: $x,
            y: 566,
            color: $this->color($canvas, $primaryColor),
            font_filename: $fontBold,
            text: $primary,
        );

        if ($secondary === null) {
            return;
        }

        $primaryWidth = (new TextWrapper($fontBold))->width($primary, fontSize: 17);
        imagettftext(
            $canvas,
            size: 17,
            angle: 0,
            x: $x + $primaryWidth + 14,
            y: 566,
            color: $this->color($canvas, self::MUTED),
            font_filename: $fontRegular,
            text: '· ' . $secondary,
        );
    }

    public function drawTitle(GdImage $canvas, string $title): int
    {
        $font = $this->resourcesDir . '/fonts/Cabin-Bold.ttf';
        $wrapper = new TextWrapper($font);
        $titleSize = 44.0;

        foreach ([44.0, 40.0, 36.0, 32.0] as $size) {
            $titleSize = $size;

            if (count($wrapper->wrap($title, $size, self::CONTENT_WIDTH)) <= 3) {
                break;
            }
        }

        $baseline = 210;

        foreach ($wrapper->wrap($title, $titleSize, self::CONTENT_WIDTH, maxLines: 3) as $line) {
            imagettftext(
                $canvas,
                size: $titleSize,
                angle: 0,
                x: self::MARGIN,
                y: $baseline,
                color: $this->color($canvas, self::WHITE),
                font_filename: $font,
                text: $line,
            );
            $baseline += (int) round($titleSize * 1.667);
        }

        return $baseline + 20;
    }
}
