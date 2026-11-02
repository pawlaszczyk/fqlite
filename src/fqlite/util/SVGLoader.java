package fqlite.util;

import fqlite.base.GUI;
import javafx.embed.swing.SwingFXUtils;
import javafx.scene.image.ImageView;

import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.ImageTranscoder;

import java.awt.image.BufferedImage;
import java.io.InputStream;

/**
 * Renders an SVG resource into a JavaFX ImageView using Batik.
 * The SVG is rasterised directly into a BufferedImage (no PNG encoding step),
 * so no additional Batik codec module is required.
 */
public class SVGLoader {

    /** Transcoder that keeps the rendered image in memory instead of writing a file. */
    private static class BufferedImageTranscoder extends ImageTranscoder {
        private BufferedImage image;

        @Override
        public BufferedImage createImage(int w, int h) {
            return new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
        }

        @Override
        public void writeImage(BufferedImage img, TranscoderOutput out) throws TranscoderException {
            this.image = img;
        }

        BufferedImage getImage() {
            return image;
        }
    }

    public static ImageView loadSvg(String resource, double width, double height)
            throws Exception {

        try (InputStream is = GUI.class.getResourceAsStream(resource)) {

            if (is == null)
                throw new IllegalArgumentException("SVG resource not found: " + resource);

            // render at twice the size so the icon stays sharp on HiDPI/Retina screens
            double scale = 2.0;

            BufferedImageTranscoder transcoder = new BufferedImageTranscoder();
            transcoder.addTranscodingHint(ImageTranscoder.KEY_WIDTH, (float) (width * scale));
            transcoder.addTranscodingHint(ImageTranscoder.KEY_HEIGHT, (float) (height * scale));
            transcoder.transcode(new TranscoderInput(is), null);

            ImageView imageView = new ImageView(SwingFXUtils.toFXImage(transcoder.getImage(), null));
            imageView.setFitWidth(width);
            imageView.setFitHeight(height);
            imageView.setPreserveRatio(true);
            imageView.setSmooth(true);

            return imageView;
        }
    }
}
